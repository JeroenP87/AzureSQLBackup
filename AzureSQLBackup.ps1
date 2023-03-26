#AzureSQLBackup
#2023.03.26 JP
#potsolutions.nl
#This code requires PowerShell 7.1!

Update-AzConfig -DisplayBreakingChangeWarning $false

try
{
    "Logging in to Azure..."
    Connect-AzAccount -Identity
}
catch {
    Write-Error -Message $_.Exception
    throw $_.Exception
}

#Azure Variables
$subscriptionid = ""

#SQL Variables
$sqlresourcegroup = ""
$sqlelasticpool = ""
$sqlserver = ""
$sqluser = ""
$secure = ConvertTo-SecureString "" -AsPlainText -Force #sqladminpass

#Storage Variables
$storageaccount = ""
$storageresourcegroup = ""
$destcontainer = ""
$deststoragekey = ""


#Local stuff
$tempdir = "C:\programdata\AzureSQLBackup"

if (test-path $tempdir) {
    Remove-Item $tempdir -Recurse -Force
}
mkdir $tempdir

Start-Transcript -Path $tempdir\sqlbackup.txt

Select-AzSubscription $subscriptionid

#Starting the backup!
$databases = Get-AzSqlDatabase -ResourceGroupName $sqlresourcegroup -ServerName $sqlserver
$date = (Get-Date).ToString('yyyy-MM-dd-HH:MM')
$context = (Get-AzStorageAccount -ResourceGroupName $storageresourcegroup | Where-Object StorageAccountName -EQ $storageaccount).Context

$databases | foreach-Object -parallel {
    $db = $_
    if ($_.DatabaseName.Equals("master")) { continue }
    if ($_.DatabaseName.Contains("COPY")) { continue }

    function ApprovePendingEndpoints {
        $storageeps = Get-AzPrivateEndpointConnection -privatelinkresourceid "subscriptions/$USING:subscriptionid/resourceGroups/$USING:storageresourcegroup/providers/Microsoft.Storage/storageAccounts/$USING:storageaccount" -erroraction silentlycontinue
        if($storageeps) {
            foreach ($storageep in $storageeps) {
                if ($storageep.PrivateLinkServiceConnectionState.Status -eq "Pending") {
                    if (Approve-AzPrivateEndpointConnection -ResourceId $storageep.Id -erroraction silentlycontinue) {
                        log "storage account Private Link Approved"
                    }
                }
            }
        }
    
        $sqleps = Get-AzPrivateEndpointConnection -privatelinkresourceid "subscriptions/$USING:subscriptionid/resourceGroups/$USING:sqlresourcegroup/providers/Microsoft.Sql/servers/$USING:sqlserver" -erroraction silentlycontinue
        if ($sqleps) {
            foreach ($sqlep in $sqleps) {
                if ($sqlep.PrivateLinkServiceConnectionState.Status -eq "Pending") {
                    if (Approve-AzPrivateEndpointConnection -ResourceId $sqlep.Id -erroraction silentlycontinue) {
                        log "SQL Private Link Approved"
                    }
                }
            }
        }
    }

    function log {
        param (
            $log,
            $sev = "info"
        )
        $message = "$((Get-Date).ToString('yyyy-MM-dd-HH:mm:ss')); $($_.DatabaseName); $($log)"

        write-output $message
        if ($sev.ToLower() -contains "error") {
            Throw $message
        }
    }

    start-sleep -Seconds (100..900 | get-random)
    if (test-path "$USING:tempdir\$($_.DatabaseName).job") { 
        log "Skipping due to dual exectution.."
        "$USING:tempdir\$($_.DatabaseName).job"
        continue 
    }
    
    Write-Output "ok" | Out-File -FilePath "$tempdir\$($_.DatabaseName).job"

    try {
        log "Starting Backup..."
        foreach ($database in $USING:databases) {
            if ($database.DatabaseName.Equals("$($db.DatabaseName)COPY")) {
                log "Deleted existing COPY Database, indicating previous backup didn't run correctly."
                Remove-AzSqlDatabase -DatabaseName "$($db.DatabaseName)COPY" -ServerName $db.ServerName -ResourceGroupName $db.ResourceGroupName -Force
                Start-Sleep -Seconds 600
            }
        }
        
        if (!(New-AzSqlDatabaseCopy -CopyDatabaseName "$($_.DatabaseName)COPY" -CopyResourceGroupName $_.ResourceGroupName -CopyServerName $_.ServerName -DatabaseName $_.DatabaseName -ResourceGroupName $_.ResourceGroupName -ServerName $_.ServerName -ElasticPoolName $USING:sqlelasticpool)) {
            log "create COPY DB Error" -sev "error"
        }
        
        $op = New-AzSqlDatabaseExport -ResourceGroupName $_.ResourceGroupName `
        -DatabaseName "$($_.DatabaseName)COPY" `
        -ServerName $_.ServerName `
        -StorageKeyType "StorageAccessKey" `
        -StorageKey $USING:deststoragekey `
        -StorageUri "https://$USING:storageaccount.blob.core.windows.net/$USING:destcontainer/$($_.ServerName)/$USING:date/$($_.DatabaseName).bacpac" `
        -AdministratorLogin $USING:sqluser `
        -AdministratorLoginPassword $USING:secure `
        -UseNetworkIsolation $true `
        -StorageAccountResourceIdForPrivateLink "subscriptions/$USING:subscriptionid/resourceGroups/$USING:storageresourcegroup/providers/Microsoft.Storage/storageAccounts/$USING:storageaccount" `
        -SqlServerResourceIdForPrivateLink "subscriptions/$USING:subscriptionid/resourceGroups/$USING:sqlresourcegroup/providers/Microsoft.Sql/servers/$USING:sqlserver"
        log "ExportID $($op.OperationStatusLink)"
               
        $loop = 1
        for ($i = 0; $loop -eq 1; $i++) {
            ApprovePendingEndpoints
            $importexportstatus = Get-AzSqlDatabaseImportExportStatus -OperationStatusLink $op.OperationStatusLink -erroraction silentlycontinue
            
            if ($i -eq 60) { 
                log $importexportstatus
                log $importexportstatus.Status
                log $importexportstatus.StatusMessage
                log $importexportstatus.ErrorMessage
                log $importexportstatus.PrivateEndpointRequestStatus[0]
                log $importexportstatus.PrivateEndpointRequestStatus[1]
                log "Backup timed-out" -sev "error"
            }
            if ($importexportstatus.GetType().Name.Equals("String")) {
                if ($importexportstatus.Contains("failed")) {
                    log $importexportstatus -sev "error"
                }
            }
            if (!($importexportstatus)) {
                Start-Sleep -Seconds 300
                continue
            }
            if ($importexportstatus.Status -eq "Failed") {
                log $importexportstatus.Status
                log "Backup failed $($op.OperationStatusLink)" -sev "error"
            }
            if (!($importexportstatus.StatusMessage.Contains("Progress = 0 %"))) {
                log "$($importexportstatus.StatusMessage)"
            }
            if ($importexportstatus.Status -eq "Succeeded") {
                $loop = 0
            } else {
                Start-Sleep -Seconds 300
            }
        }
        log "Copy is done"

        start-sleep -Seconds 60
        for ($i = 0; !(Get-AzStorageBlob -Blob "$($_.ServerName)/$USING:date/$($_.DatabaseName).bacpac" -Container $USING:destcontainer -Context $USING:context -erroraction silentlycontinue); $i++) {
            Start-Sleep -Seconds 120
            if ($i -eq 15) { 
                $importexportstatus = Get-AzSqlDatabaseImportExportStatus -OperationStatusLink $op.OperationStatusLink -erroraction silentlycontinue
                log $importexportstatus
                log $importexportstatus.Status
                log $importexportstatus.StatusMessage
                log $importexportstatus.ErrorMessage
                log $importexportstatus.PrivateEndpointRequestStatus[0]
                log $importexportstatus.PrivateEndpointRequestStatus[1]
                log "No bacpac found after 30min" -sev "error"
            }
        }
        log "pacpac exists"
        
        for ($i = 0; (Get-AzStorageBlobCopyState -Blob "$($_.ServerName)/$USING:date/$($_.DatabaseName).bacpac" -Container $USING:destcontainer -Context $USING:context -erroraction silentlycontinue); $i++) {
            Start-Sleep -Seconds 120
            if ($i -eq 200) { 
                log "Copy state not ok" -sev "error"
            }
        }
        Remove-AzSqlDatabase -DatabaseName "$($_.DatabaseName)COPY" -ServerName $_.ServerName -ResourceGroupName $_.ResourceGroupName -Force | out-null
        log "COPY DB Removed"
    }
    catch {
        Write-Error "$((Get-Date).ToString('yyyy-MM-dd-HH:mm:ss')); $($db.DatabaseName); Error backing up DB"
        Write-Error -Message $_.Exception
        Remove-AzSqlDatabase -DatabaseName "$($db.DatabaseName)COPY" -ServerName $db.ServerName -ResourceGroupName $db.ResourceGroupName -Force | out-null
    }
} -ThrottleLimit 5
write-output "$((Get-Date).ToString('yyyy-MM-dd-HH:mm:ss'));; JOB FINISHED"

Stop-Transcript
