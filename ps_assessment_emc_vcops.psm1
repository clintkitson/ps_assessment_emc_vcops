##
#
# VMware - Virtu-Al.Net - @alanrenouf
# EMC - vElemental.com -  @clintonskitson
# 09/19/2014
# ps_vcops.psm1 - module for pulling metrics from vCOps
#
##

# 1st load the module
# dir ps_vcops.psm1 | import-module

# Connect to vCenter server being managed
# Connect-VIServer xxx -user root -Password xx

# Connect to vCOps server
# Connect-vCOpsServer -Server mgmt-vcops01 -Username admin -Password pass

# Get a specific metric for a host
# Get-VMHost | Select -First 1 | Get-vCOpsResourceMetric -metricKey "badge|alert_count_critical"

# Get many metrics for a host
# Get-VMHost | Select -First 1 | Get-vCOpsResourceMetric -metricKey "badge|alert_count_critical","badge|health"

# Get all metrics (attributes) for a resource in the past 10 Minutes
# Get-VMHost | Select -First 1 | Get-vCOpsResourceMetric -startDate (Get-Date).AddMinutes(-10)

# Get all resource attributes available 
# Get-VMHost | Select -First 1 | Get-vCOpsResourceAttribute

# Custom vCOps DB Query
# Get-vCOpsDBQuery (see example in module)


#Get-datacenter brs_vlab_seven | get-datastore vmax_1342* | Get-vCOpsResourceAttribute | %{ $_.attr_key } | select -unique | sort | Out-File datastore.txt

#Get-vmhost * | select -first 1 |Get-vCOpsResourceMetricsAll -startdate "01/12/2014 12:00AM" -endDate "01/17/2014 2:00PM" -metricKeyMatch (gc attr_vmhost.txt)

#New-vCOpsAssessment -startdate "01/12/2014 12:00AM" -endDate "01/17/2014 9:00PM" -hashResources (Invoke-Expression ((gc attr_resources.txt) -join "`n"))



Function Get-MD5Sum {
    [CmdletBinding()]
    param([Parameter(Mandatory=$True, Position=0, ValueFromPipeline=$true)]
        [psobject]$InputObject)
    Process {
        $path = $InputObject.FullName
        $md5 = new-object -TypeName System.Security.Cryptography.MD5CryptoServiceProvider
        $hash = [System.BitConverter]::ToString($md5.ComputeHash([System.IO.File]::ReadAllBytes($path))) -replace "-",""
        New-Object -type psobject -property @{path=$path;md5sum=$hash}
    }
}

$global:spath = (Split-Path -parent $MyInvocation.MyCommand.Definition)

$script:moduleMd5 = Get-item $myInvocation.MyCommand.Path | Get-MD5Sum | Select Md5Sum 
$script:moduleMd5 | select @{n="$($myInvocation.MyCommand.name) md5Sum";e={$_.md5Sum}}| Format-Table * -autosize | Out-String | Write-host

$Host.UI.RawUI.windowTitle = "EMC vCOps Assessment Collection pid$($pid)"

$Version = Get-PSSnapin vmware.vimautomation.core | %{ $_.version }
if(!$Version -or $Version.Major -lt 5 -or ($Version.Major -eq 5 -and $Version.Minor -lt 1)) { Write-Error "Missing proper PowerCLI version (v5.1+).";pause;break }
if($PSVersionTable.PSVersion.Major -lt 3) { Write-Error "Version 3 of Powershell required.";pause;break }


function Get-vCOpsAssessmentVersion {
    New-Object -Type "psobject" -Property @{Name="assessment.emc.vcops";version="0.1";majorversion=0;minorversion=1;md5Sum=$script:moduleMd5.md5Sum}
}


Function Read-HostCustom {
    param($description,[switch]$booleanQuestion)
        if($booleanQuestion) {
            do {
                $readHost = Read-Host "`nWould you like to $($description) (y/n)?"
            } until (@("yes","y","n","no") -contains $readHost)
            $readHost = if(@("yes","y") -contains $readHost) { $True } else { $False }
        } else {
            do {
                $readHost = Read-Host "`nPlease enter a $($description) for the collection"
                $readHost2 = Read-Host "Is `"$($readHost)`" correct (y/n)?"
            } until (@("yes","y") -contains $readHost2)
        }
    Return $readHost
}

#New-vCOpsAssessmentFromDateRanges -arrDateRange (import-csv 'X:\kitsoc\proxy testing\runtimes.csv')
function New-vCOpsAssessmentFromDateRanges {
    [CmdletBinding()]
    Param([array]$arrDateRange=$(throw "missing -arrDateRange"))
    
    $guid=$(([system.guid]::newguid().guid).split('-')[-1])
    $hashResources = (Invoke-Expression ((gc "$($global:spath)\attr_resources.txt") -join "`n"))    
    
    $arrDateRange | %{
	$hashResourcesClone = $hashResources.clone()
	$dateRange = $_
	$commandKeys = $hashResourcesClone.command.keys | %{ $_ }
        $commandKeys | %{
	    $commandKey = $_
            if($dateRange.$commandKey) { $hashResourcesClone.command.$commandKey = Invoke-Expression "{$($dateRange.$commandKey)}" }
        }

	New-vCOpsAssessment -noWindow -startdate $_.startDate -endDate $_.endDate -AssessmentName $_.name -guid $guid -hashResources $hashResourcesClone
    } | Tee-Object -variable arrLog
    New-vCOpsAssessmentsSummary -destDir "$($env:TEMP)\vCOpsAssessment-$($guid)\" -files (Get-ChildItem ($arrLog | where {$_.type -eq "consolidatedMeasuredResults"} | %{ $_.file})) -guid $guid
    Invoke-Item "$($env:TEMP)\vCOpsAssessment-$($guid)\"
}

function New-vCOpsAssessment {
    [CmdletBinding()]
    Param(
        [ValidateScript({if($_ -match "-") { throw "Name cannot include '-'"} else { $true }})]
	$AssessmentName=$(Read-HostCustom "name"),
        $hashResources=$(Invoke-Expression ((gc "$($global:spath)\attr_resources.txt") -join "`n")),
	[datetime]$startDate=$(Read-HostCustom "startDate"),
	[datetime]$endDate=$(Read-HostCustom "endDate"),
	$VIServer=$(Check-VIServer),
	$vCOpsServer=$(Check-vCOpsServer),
	$guid,
	[switch]$noWindow
    )

    if(!$global:DefaultVIServer -or ($global:DefaultVIServer -and !$global:DefaultVIServer.IsConnected)) {
        Throw "Missing connection to VIServer, run Connect-VIServer -server ip/dns"
    }

    if($global:DefaultVIServers.count -gt 1) { 
        Throw "Connected to more than one vCenter instance, please close and reopen PowerCLI."
    }

    $arrFiles = @()

    $fileDateStart = ($startDate).toString("yyyMMddhhmm")
    $fileDateEnd = ($endDate).toString("yyyMMddhhmm")
    $fileName = "vCOpsAssessment-$($AssessmentName)-$($fileDateStart)$($fileDateEnd)-$(([system.guid]::newguid().guid).split('-')[-1])"
    if($guid) { $guid = "vCOpsAssessment-$($guid)\" }
    $filePath = "$($env:TEMP)\$($guid)$($fileName)"
    $ZipFile ="$($filePath)\$($fileName).zip"

    try {
        $dirPath_Root = New-Item -type directory $filePath
    } catch {
        Throw $_
    }
    try {
        $dirPath = New-Item -type directory "$($dirPath_Root.FullName)\content"
    } catch {
        Throw $_
    }

    $hashCommandResults = [ordered]@{}
    $hashResources.command.keys | %{
        $commandKey = $_
	try {
    		$hashCommandResults.$commandKey = .$hashResources.command.$commandKey
    	} catch {
		Write-Host -fore red ($error[0] | fl * -force | out-string)
		pause
	}
    }

    $hashResources.command.keys | %{
	$commandKey = $_	
	[array]$arrHashLogCommandKey = @()
        try {
                #$results = Invoke-Expression $tmpHash.Command
        $global:hashLog = @{}
	$global:hashLog.command = $commandKey
        $hashCommandResults.$commandKey | %{
	    $global:hashLog = @{}
	    $command = $_
            $global:hashLog.key = $commandKey
	    #$global:hashLog.Command = $tmpHash.$Command

            $global:hashLog.StartDate = Get-Date
	    $name = if($_.resourceName) {$_.resourceName} else {$_.name}
	    $global:hashLog.Name = $name
	    $fileName = "$($dirPath)\$($commandKey)-$($AssessmentName)-$($name).csv"
	    $global:hashLog.File = $fileName
	Using-Culture en-us {
	    $_ | Get-vCOpsResourceMetricsAll -startDate $startDate -endDate $endDate -metricKey $hashResources.metricKey.$commandKey -metricKeyMatch $hashResources.metricKeyMatch.$commandKey | export-csv -notypeinformation $fileName
	}
	    $global:hashLog.EndDate = Get-Date
	    $global:hashLog.DurationSeconds = ($hashLog.EndDate - $hashLog.StartDate).TotalSeconds
	    $global:hashLog.Error = $False
	    New-Object -type PsObject -Property $global:hashLog 
	    [array]$arrHashLogCommandKey += New-Object -type PsObject -Property $global:hashLog 
	}
		    	
	    
        } catch {
            Write-Host -fore red ($error[0] | fl * -force | out-string)
            Write-Error "Problem when executing $($command) $($tmpHash.name)"
            $global:hashLog.Error = $True
            #if(!$tmpHash.CanError) {
                Write-Host -fore "red" "Pausing due to error."
                pause
	        New-Object -type PsObject -Property $global:hashLog
            #}
        }
	Measure-vCOpsAssessment -AssessmentName $AssessmentName -destdir $dirPath -files (Get-Item $arrHashLogCommandKey.file) | Tee-Object -variable tmpHashLog
        Measure-vCOpsAssessmentMeasured -AssessmentName $AssessmentName -destdir $dirPath -files (Get-Item ($tmpHashLog.file))
    } | Tee-Object -variable AssessmentLog

    Write-Host "Creating ZIP File"
    $AssessmentLOg | %{ $_.File } | where {$_} | Out-Zip -ZipFile $ZipFile
    if(!$noWindow) { Invoke-Item $filePath }
    Write-Host "VMware vCOps Assessment completed`n`n"

}

#Get-vCOpsResource -pattern "name=seven" | where {$_.resourceKindKey -eq "VMwareAdapter Instance"}
function Get-vCOpsResource {
    [CmdletBinding()]
    param(
	[regex]$pattern=".*"
    )
    $defaultvCOpsServer.Data | select-string -pattern $pattern | %{
	[array]$arrData = $_ -split "\&"
	$hashResource = @{}
	$arrData | %{
		$resource = $_ -split "="
		if($resource[0]) { $hashResource.($resource[0]) = $resource[1] }
		
	}
	New-Object -type psobject -property $hashResource
		
    }
}


function Get-vCOpsResourceMetricsAll {
    [CmdletBinding()]
    param(
        [Parameter(ValueFromPipeline=$true,Mandatory=$true,Position=0)]
	[psobject[]]$item,
	[datetime]$startDate = (Get-Date).AddMinutes(-10),
        [datetime]$endDate = (Get-Date),
	$metricKeyMatch,
	$metricKey
    )
    Begin {
        $hashNewMetrics = [ordered]@{}
	$hashMetrics = @{}
        [array]$arrNewResults = @()
	[array]$arrItem = @()
    }
    Process {
	$arrItem += $item
    }
    End {

    [array]$arrOutput = %{ $arrItem | %{
	$item = $_
        [array]$attributes = $item | Get-vCOpsResourceAttribute

        $hashAttributes = @{}
        $attributes | %{ $hashAttributes.($_.attr_key) = $_.friendlyname }

	if($metricKeyMatch) { $metricKey = $null } elseif($metricKey) {} else { $metricKey = $attributes.attr_key }
        $hashResults = [ordered]@{}
        $results = $item | Get-vCOpsResourceMetric -metricKey ($metricKey) -metricKeyMatch $metricKeyMatch -startDate $startdate -endDate $endDate
        $results | %{
            if(!$hashResults.($_.date)) { 
		$hashResults.($_.date) = @{} 
	        $hashResults.($_.date).Add("date",$_.date)
		$hashResults.($_.date).Add("name",$_.name)
	    }

	    if(!$hashMetrics.($_.metricKey)) {
		$instance = $_.metricKey.split('|')[0]
	        $instanceName = if($Global:DefaultvCOPsServer.resourceLookup.$instance) { "||$($Global:DefaultvCOPsServer.resourceLookup.$instance.FriendlyName)" } else { "" }
		$hashMetrics.($_.metricKey) = $instanceName
	    } else {
		$instanceName = $hashMetrics.($_.metricKey)
	    }
	    $newName = "$($_.metricKey)||$($hashAttributes.($_.metrickey))$($instanceName)"
	    $hashNewMetrics.($newName) = 1
	    $hashResults.($_.date).Add($newName,$_.value)
        }
        %{ for($i=0;$i -le $hashresults.count;$i++) { [pscustomobject]$hashresults[$i] } } | sort date
    } }

    $arrOutput | Select -Property (("name","date",($hashNewMetrics.keys | sort)) | %{$_})

    }
}


Function Connect-vCOpsServer {
     Param (
           $Server,
           $Username,
           $Password,
	   $credential
     )
     Process {
	if((!$username -or !$password) -and !$credential) {
		$credential = Get-Credential
	}
	if($credential) { 
		$username = $credential.username
		$password = $credential.GetNetworkCredential().password
	}
	
           $URL = "https://$Server/HttpPostAdapter/OpenAPIServlet"
           $http_request = New-Object System.Net.WebClient
           $http_request.Credentials = (New-Object System.Net.NetworkCredential($Username,$Password))
           [System.Net.ServicePointManager]::ServerCertificateValidationCallback = {$true}
           $Send = 'action=lookupResource&resourceName=regex:.*'
           try {
           $Data = $http_request.UploadString($URL,$Send)
           }
           catch [Net.WebException] {
           Write-Error "Unable to connect to $Server, please verify connection information and try again"
                Write-Error "$($_.Exception)"
                Return
           }

	$Connection = New-Object PSObject -Property @{            
             Server = $Server
             Username = $Username
                Password = $Password
                Data = ($data -split "`n")
		ResourceLookup = $null
                APIURL = ("https://$Server/HttpPostAdapter/OpenAPIServlet")
         }  
	$Global:DefaultvCOPsServer = $Connection

	   [array]$arrResourceLookup = Get-vCOpsDBQuery -Username $connection.Username -Password $connection.Password -Server $connection.Server -sqlQuery "select resource_id,name,resourcekind.resknd_key as resknd_key from aliveresource,resourcekind where ALIVERESOURCE.RESKND_ID = RESOURCEKIND.RESKND_ID"
	   $hashResourceLookup = @{}
	   $arrResourceLookup | %{ $hashResourceLookup.("$($_.resknd_key):$($_.resource_id)") = $_ | Select * -excludeproperty name }
           
	   $Global:DefaultvCOPsServer.ResourceLookup = $hashResourceLookup
           [System.Net.ServicePointManager]::ServerCertificateValidationCallback = {$false}
           Write-Host "Connected to $($DefaultvCOPsServer.Server)"
     }
}

Function Get-vCOpsResourceMetric {
     # Get-VMHost | Select -First 1 | Get-vCOpsResourceMetric 
     # Get-VMHost | Select -First 1 | Get-vCOpsResourceMetric -Verbose
     # Get-VMHost | Select -First 1 | Get-vCOpsResourceMetric -metricKey "badge|alert_count_critical"
     # Get-VMHost | Select -First 1 | Get-vCOpsResourceMetric -startDate (Get-Date).AddMinutes(-10)
     # (Get-Datastore)[0] | Get-vCOpsResourceMetric -metricKey "badge|alert_count_critical","badge|alert_count_immediate"
     [CmdletBinding()]
     Param (
           $metricKey,
           $metricKeyMatch,
           [datetime]$startDate = (Get-Date).AddMinutes(-10),
           [datetime]$endDate = (Get-Date),
           [Parameter(Mandatory=$true, Position=0, ValueFromPipeline=$true)]
           [PSObject[]]$PsObject,
           [switch]$includeDt=$true,
           [switch]$includeSmooth=$true
     )
     Begin {
        $arrInput = @()
     }
     Process {
        [array]$arrInput += $PsObject
     }
     End {
        $arrInput | Foreach {
           $tmpPsObject = $_
           if (-not $DefaultvCOPsServer) {
                Write-Error "No connection to a vCOps Server found, please use Connect-vCOpsServer to connect to a server"
                Return
           } Else {
		        if($_.adapterKindKey) {
		            $adapterKindKey = $_.adapterKindKey
		            $resourceKindKey = $_.resourceKindKey -replace " ",'%20'
		            $identifiers = $_.identifiers
		            $resourceName = $_.resourceName
		        } else {
		            $adapterKindKey = "VMWARE"
		            $resourceKindKey = $_.ExtensionData.MoRef.Type
		            $identifiers = "VMEntityObjectID::$($_.ExtensionData.MoRef.Value)`$`$VMEntityVCID::$($global:DefaultVIServer.ExtensionData.Content.About.InstanceUUID)"
		            $resourceName = $_.name
		        }
                $resourceParams = "resourceName=$($resourceName)&adapterKindKey=$($adapterKindKey)&resourceKindKey=$($resourceKindKey)&identifiers=$($identifiers)"
                Write-Verbose $resourceParams
                $startTime = [math]::round(([decimal](($startDate).ToUniversalTime() | Get-Date -UFormat "%s")*1000),0)
                $endTime = [math]::round(([decimal](($endDate).ToUniversalTime() | Get-Date -UFormat "%s")*1000),0)
                
                ## Loop through the attributes for the resource to retrieve
                
		        [array]$arrAttributes = %{
                    if(!$metricKey -and !$metricKeyMatch) { 
                        $attribs = $tmpPsObject | Get-vCOpsResourceAttribute
                        $attribs
                    } else {
                        if($metricKey -is [array]) {
                            $metricKey | Foreach { New-Object -type PsObject -property @{"attr_key"=$_} }
                        }elseif($metricKey) {
                            New-Object -type PsObject -property @{"attr_key"=$metricKey}
                        }elseif($metricKeyMatch) {
			                $regMatch = ($metricKeyMatch -join "|")
                            $attribs = $tmpPsObject | Get-vCOpsResourceAttribute
			                $attribs | where {$_.attr_key -match $regMatch} | select attr_key
			            }
                    }
                }
		    if($arrAttributes.count -eq 0) { write-warning "Zero results with matching keys";return }
                
		    $i=0
            $arrAttributes | %{
		    $i++
		    $pct = ((($i)/$arrAttributes.count)*100)
		    if(!$tmpPsObject.name) { $tmpPsobject | Add-Member -membertype noteproperty -name name -value $tmpPsObject.resourceName -ea 0 }
		    write-verbose $tmpPsObject
		    write-progress -status $tmpPsObject.name -activity "Collecting $($_.attr_key) $($i)/$($arrAttributes.count)" -PercentComplete $pct
                    $MetricKey = $_.attr_key
                    $Query = "action=getMetricDataAndDT&$($resourceParams)&metricKey=$($MetricKey)&starttime=$($StartTime)&endtime=$EndTime"
                    if(!$includeDt) { $Query += "&includeDt=false" }
                    if(!$includeSmooth) { $Query += "&includeSmooth=false" }
                    Write-Verbose $Query
                    $http_request = new-object System.Net.WebClient
		    $http_request.Headers.Add("Cookie",$global:jsessionid)
                    $http_request.Credentials = (New-Object System.Net.NetworkCredential($DefaultvCOPsServer.Username,$DefaultvCOPsServer.Password))
                    [System.Net.ServicePointManager]::ServerCertificateValidationCallback = {$true}
                    $Results = $http_request.UploadString($DefaultvCOpsServer.APIURL,$Query)
                    If ($Results.Length -eq 36) {
                         # No Results Returned
                         Write-Verbose "No Results Found"
                         Return
                    } Else {
                         $MyObj = @()
                         $data = $Results -split "`n"
                         [array]$arrHeaders = $data[0] -split ", "
                         $data[1..($data.count-1)] | Foreach { 
                             [array]$arrTemp = $_ -split ","
                             $tmpHash = @{}
                             0..($arrTemp.count-1) | Foreach { $tmpHash.($arrHeaders[$_]) = $arrTemp[$_] }
			if($tmpHash.time) { 
                             #$Object = New-Object -type psobject -property $tmpHash
                               #$Object | Add-Member -membertype noteproperty -name Name -value $tmpPSObject.Name
                               #$Object | Add-Member -membertype noteproperty -name MetricKey -value $MetricKey
				$tmpHash.Name = $tmpPSObject.Name
				$tmpHash.MetricKey = $MetricKey
				$tmpHash.Date = ([timezone]::CurrentTimeZone.ToLocalTime(([datetime]'1/1/1970').AddSeconds([Math]::Floor($tmpHash.Time /1000))))
                               #$MyObj += $Object | Select *, @{N="Date";E={([timezone]::CurrentTimeZone.ToLocalTime(([datetime]'1/1/1970').AddSeconds([Math]::Floor($_.Time /1000))))}}
			       $tmpHash.Remove("Time")
				[pscustomobject]$tmpHash
			}

                         }
                         #$MyObj | Where { $_.Time } | Select * -ExcludeProperty Time
                    }
                }
            }
        } 
	[System.Net.ServicePointManager]::ServerCertificateValidationCallback = {$false}
     }
}



Function Get-vCOpsResourceAttribute {
    #(Get-VMHost)[0] | Get-vCOpsResourceAttribute
    [CmdletBinding()]
     Param (
           $sqlQuery,
           [Parameter(Mandatory=$true, Position=0, ValueFromPipeline=$true)]
           [PSObject[]]$PsObject     
     )
     Begin {
        if(!$global:DefaultvCOpsServer) { Write-Host -fore red "Not Logged into a vCOps server" }
        $arrInput = @()
     }
     Process {
        [array]$arrInput += $PsObject
     }
     End {
        
        $sqlQuery = @"
SELECT LOCALIZED_NAME.name,attr_key,rkattrib_key
FROM (
SELECT ALIVERESOURCE.resource_id,ALIVERESOURCE.resknd_id,ALIVERESOURCE.name,array_agg(stringVal) AS IDENTIFIERS 
 FROM ALIVERESOURCE,RESOURCEKIND,ADAPTERKIND,RESOURCEKINDIDENT,RESOURCEIDENTIFIER
 WHERE ALIVERESOURCE.NAME = '<resourceName>'
 AND ALIVERESOURCE.RESKND_ID = RESOURCEKIND.RESKND_ID
 AND ALIVERESOURCE.current_health != 4
<sqlQueryResourceKindKey>
 AND RESOURCEKIND.ADAPTER_KIND_ID = ADAPTERKIND.ADAPTER_KIND_ID
 AND RESOURCEKINDIDENT.ADAPTER_KIND_ID = ADAPTERKIND.ADAPTER_KIND_ID
 AND RESOURCEKINDIDENT.RESKND_ID = RESOURCEKIND.RESKND_ID
 AND RESOURCEKINDIDENT.TYPE IS NULL
 AND RESOURCEKINDIDENT.RKIDENT_ID = RESOURCEIDENTIFIER.RKIDENT_ID
 AND ALIVERESOURCE.RESOURCE_ID = RESOURCEIDENTIFIER.RESOURCE_ID
<sqlQueryIdent>
 GROUP BY ALIVERESOURCE.resource_id,ALIVERESOURCE.resknd_id,ALIVERESOURCE.name
) AS t,RESOURCEATTRIBUTEKEY,ATTRIBUTEKEY,RESOURCEKINDATTRIBUTE,LOCALIZED_NAME
WHERE  RESOURCEATTRIBUTEKEY.resource_id = t.resource_id
AND RESOURCEATTRIBUTEKEY.attrkey_id = ATTRIBUTEKEY.attrkey_id
AND RESOURCEATTRIBUTEKEY.rkattrib_id = RESOURCEKINDATTRIBUTE.rkattrib_id
AND LOCALIZED_NAME.name_id = RESOURCEKINDATTRIBUTE.name_id AND LOCALIZED_NAME.adapter_kind_id = RESOURCEKINDATTRIBUTE.adapter_kind_id AND lang_id = 1
order by attr_key
"@
	if($arrInput[0].name) { 
$sqlQueryIdent = @"
 AND ((rkident_key = '<identName1>' AND stringVal = '<identValue1>') OR
      (rkident_key = '<identName2>' AND stringVal = '<identValue2>'))
"@
}else {
$sqlQueryResourceKindKey = @"
 AND RESOURCEKIND.resknd_key = '<resourceKindKey>'
"@
}
	$sqlQuery = $sqlQuery -replace '<sqlQueryIdent>',$sqlQueryIdent
	$sqlQuery = $sqlQuery -replace '<sqlQueryResourceKindKey>',$sqlQueryResourceKindKey

        $arrInput  | Foreach {
            if($_.resourceName) {
		$tmpPSObject = New-Object -type PSObject -property @{resourceName = $_.resourceName; resourceKindKey = $_.resourceKindKey}
	    
	    } else {
	    $tmpPSObject = New-Object -type PSObject -property @{resourceName = $_.Name;
                                                                 identValue1 = ($_.ExtensionData.MoRef -split "-" | select -last 2) -join "-";
                                                                 identValue2 = $global:DefaultVIServer.ExtensionData.Content.About.InstanceUUID;
                                                                 identName1 = "VMEntityObjectID";
                                                                 identName2 = "VMEntityVCID"}                                                                 
            }
            $tmpPSObject

	    write-verbose $sqlQuery		
        } | Get-vCOpsDBQuery -Username $global:defaultvCOPsServer.Username -Password $global:defaultvCOPsServer.Password -Server $global:defaultvCOPsServer.Server -sqlQuery $sqlQuery
    }
}



Function Get-vCOpsDBQuery {
    # New-Object -type PSObject -property @{resourceName = $_.Name; identValue1 = ($_.ExtensionData.MoRef -split "-" | select -last 2) -join "-"; identValue2 = $global:DefaultVIServer.ExtensionData.Content.About.InstanceUUID;identName1 = "VMEntityObjectID"; identName2 = "VMEntityVCID"} |
    #     Get-vCOpsDBQuery -Username $global:defaultvCOPsServer.Username -Password $global:defaultvCOPsServer.Password -Server $global:defaultvCOPsServer.Server -sqlQuery $sqlQuery
    
    [CmdletBinding()]
    Param (
        $Username,
        $Password,
        $Server,
        $sqlQuery=$(Write-Error "Missing -sqlquery";break),
       [Parameter(Mandatory=$false, Position=0, ValueFromPipeline=$true)]
       [PSObject[]]$PsObject 
    )
    Begin {
        if(!$global:DefaultvCOPsServer) { Write-Host -fore red "Not Logged into a vCOps server" }
        $arrInput = @()
        # Remove an old jsessionid if it is around 
        $global:jsessionid = $null

        $URL = "https://$($Server)/vcops-custom/"

        [System.Reflection.Assembly]::LoadWithPartialName("System.Web") | out-null
        $netAssembly = [Reflection.Assembly]::GetAssembly([System.Net.Configuration.SettingsSection])
        IF($netAssembly) {
            $bindingFlags = [Reflection.BindingFlags] "Static,GetProperty,NonPublic"
            $settingsType = $netAssembly.GetType("System.Net.Configuration.SettingsSectionInternal")
            $instance = $settingsType.InvokeMember("Section", $bindingFlags, $null, $null, @())
            if($instance) {
                $bindingFlags = "NonPublic","Instance"
                $useUnsafeHeaderParsingField = $settingsType.GetField("useUnsafeHeaderParsing", $bindingFlags)
                if($useUnsafeHeaderParsingField) {
                    $useUnsafeHeaderParsingField.SetValue($instance, $true) | out-null
                }
            }
        }
        [System.Net.ServicePointManager]::ServerCertificateValidationCallback = {$true}
        [System.Net.ServicePointManager]::Expect100Continue = $false

        $http_request = new-object System.Net.WebClient

        function Invoke_Http {
            param($URL,$sendParams,$cookie)
            Write-Verbose "$($URL) $($global:jsessionid) $($global:cookie) $($sendParams)"
            $webclient = New-Object System.Net.WebClient
            $webclient.Headers.Add("Accept-Encoding: gzip,deflate")
            $webclient.Headers.Add("Content-type: application/x-www-form-urlencoded")
            $webclient.Headers.Add("Cache-Control: no-cache")
            
            if($global:jsessionid) { 
                $tmpCookie = if($cookie) { "$($global:jsessionid); $($cookie)" -replace ";^","" } else { $global:jsessionid }
                $webclient.Headers.Add("Cookie",$tmpCookie)
            }

            $tmpOut = if($sendParams) { $webclient.UploadString($URL,$sendParams) } else { $webclient.DownloadString($URL) } 
            #Write-Verbose ($tmpOut | out-string)
            if($webclient.ResponseHeaders.Get("set-cookie") -match "JSESSIONID") {
                $tmpSetCookie = $webclient.ResponseHeaders.Get("set-cookie") -split ";" 
                Write-Verbose ($tmpSetCookie | Out-String)
                $global:jsessionid = $tmpSetCookie | where {$_ -match "JSESSIONID"}
                Write-Verbose "New Cookie: $($global:jsessionid)"
            }
            $tmpOut
        }

        ## Initial login to request Cookie
        Invoke_Http -URL $URL | Out-Null

        ## Do licensing check
        $tmpTime = [math]::round(([decimal]((Get-Date).ToUniversalTime() | Get-Date -UFormat "%s")*1000),0)
        $tmpJson = Invoke_Http -URL "$($URL)licenseCheck.naaction?ms=$($tmpTime)" -sendParams "mainAction=checkLicense"

        try { 
            $tmpResponse = $tmpJson | ConvertFrom-Json 
            if($tmpResponse.licenseState -ne "Licensed") { Write-Error "Licensing problem: $tmpResponse.licenseState";break }
        } catch {
            Write-Error "Problem parsing licensing request"
        } 
        Write-Verbose "License check: passed"

        ## Login with credentials
        $username = [System.Web.HttpUtility]::UrlPathEncode($username)
        $password = [System.Web.HttpUtility]::UrlPathEncode($password)
        $tmpTime = [math]::round(([decimal]((Get-Date).ToUniversalTime() | Get-Date -UFormat "%s")*1000),0)
        Invoke_Http -URL "$($URL)j_security_check?_dc=$($tmpTime)" -sendParams "j_username=$($username)&j_password=$($password)" -cookie "timezone=420; timezonerawoffset=-25200000;" | Out-Null

        ## Get new cookie
        Invoke_Http -URL $URL | Out-Null
    }
    Process {
        [array]$arrInput += $PsObject
    }
    End {
        $arrInput | Foreach {
            $tmpPSObject = $_
            ## Customize sql query
	    $newSqlQuery = $sqlQuery
            @("resourceName","resourceKindKey","identValue1","identValue2","identName1","identName2") | %{
                $tmpName = $_
                $newSqlQuery = $newSqlQuery -replace ("<$($tmpName)>"),($tmpPSObject.$_)
            }

            ## Request rows
            $sqlStatement = [System.Web.HttpUtility]::UrlPathEncode($newSqlQuery)
            $tmpSend = "start=0&limit=200000&mainAction=getResultSet&sql=$($sqlStatement)"
            $tmpJson = Invoke_Http -URL "$($URL)dbAccessQuery.action" -cookie "timezone=420; timezonerawoffset=-25200000;" -sendParams $tmpSend
	    $tmpResponse = $tmpJson | ConvertFrom-Json 
            $tmpResponse | where {$_.resultSet} | %{ $_.resultSet } | Select @{n="name";e={$tmpPSObject.resourceName}},@{n="FriendlyName";e={$_.Name}},* -ExcludeProperty Name
        }
	[System.Net.ServicePointManager]::ServerCertificateValidationCallback = {$false}
    }

}



Function Out-Zip {
    [CmdletBinding()]
    Param(
        [Parameter(Mandatory=$True, Position=1, ValueFromPipeline=$true)]
        [PSObject[]]$File,
        $ZipFile=$(Throw "Missing -ZipFile")
    )
    Begin {
        $arrFiles = @()
    }
    Process {
        [array]$arrFiles += $File
    }
    End {
        
        if(Test-Path $ZipFile) {
            Throw "File $($ZipFile) already exists."
        }
        Set-Content $ZipFile ("PK" + [char]5 + [char]6 + ("$([char]0)" * 18))
        $comObject = New-Object -com shell.application
        $GIZipFile = Get-Item $ZipFile
        $comObjectNS = $comObject.NameSpace(($GIZipFile).FullName)
        $arrFiles | %{ 
            Write-Host "Adding $($_) to $($GIZipFIle.FullName)"
            $copy = 0
            do { 
                if($copy -eq 0) {
                    $comObjectNS.CopyHere((Get-Item $_).FullName)
                }
                $copy++
                sleep 1
        		[array]$arrNames = $comObjectNS.Items() | %{ new-object System.IO.FileInfo "$($_.Name)" } | %{ $_.baseName }
            }until($arrNames -contains (Get-Item $_).Basename -or $copy -ge 480)
        
            if($copy -ge 10) {
                Throw "Problem creating ZIP, create the ZIP manually of the files inside the contents\ directory."
            }
        }
    }
}



Function Check-VIServer {
    if(!$global:DefaultVIServer -or ($global:DefaultVIServer -and !$global:DefaultVIServer.IsConnected)) {
        $VIServer = Read-HostCustom "vCenter Instance IP Address or Valid DNS Name"
        Write-Host "Connecting to $($VIServer) .. Please wait for secure authentication window to Virtual Center, if not logged in as account with access to VC."
        try { 
            Connect-VIServer -server $VIServer -wa 0 -ea stop
        } catch {
            Write-Error $_
            Pause
            Break
        }
    } else {
        return $global:defaultVIServer
    }
}
	


Function Check-vCOpsServer {
    if(!$global:DefaultvCOpsServer) {
        $vCOpsServer = Read-HostCustom "vCOps Instance IP Address or Valid DNS Name"
        Write-Host "Connecting to $($vCOpsServer)"
        try { 
            Connect-vCOpsServer -server $vCOpsServer -wa 0 -ea stop
        } catch {
            Write-Error $_
            Pause
            Break
        }
    } else {
        return $global:defaultvCOpsServer
    }
}


#http://stackoverflow.com/questions/2379514/powershell-formatting-values-in-another-culture
function Using-Culture ([System.Globalization.CultureInfo]$culture =(throw "USAGE: Using-Culture -Culture culture -Script {scriptblock}"),
                        [ScriptBlock]$script=(throw "USAGE: Using-Culture -Culture culture -Script {scriptblock}"))
{    
    $OldCulture = [System.Threading.Thread]::CurrentThread.CurrentCulture
    $OldUICulture = [System.Threading.Thread]::CurrentThread.CurrentUICulture
    $culture.datetimeformat.set_ShortDatePattern("yyyy-MM-ddTHH:mm:ss.fffffffzzz")
    $culture.datetimeformat.set_LongTimePattern("")
    try {
        [System.Threading.Thread]::CurrentThread.CurrentCulture = $culture
        [System.Threading.Thread]::CurrentThread.CurrentUICulture = $culture        
        Invoke-Command $script    
    }    
    finally {        
        [System.Threading.Thread]::CurrentThread.CurrentCulture = $OldCulture        
        [System.Threading.Thread]::CurrentThread.CurrentUICulture = $OldUICulture    
    }    
}


Function Measure-vCOpsAssessmentZip {
    [CmdletBinding()]
    param([Parameter(Mandatory=$True, Position=0, ValueFromPipeline=$true)]
        [System.IO.FileInfo]$FileInfo)
    Process {
	[System.Reflection.Assembly]::LoadWithPartialName("System.IO.Compression.FileSystem") | Out-Null 

	$destDir  = "$($env:temp)\$(([system.guid]::newguid().guid).split('-')[-1])"

        [System.IO.Compression.ZipFile]::ExtractToDirectory("$($FileInfo.FullName)", "$destDir")  | out-null
	Measure-vCOpsAssessment -destDir $destDir
	Invoke-Item $destDir
    }

}


Function Measure-vCOpsAssessment {
    [CmdletBinding()]
    param($destDir=$(throw "missing -destDir"),[System.IO.FileInfo[]]$files,$AssessmentName)
    Process {
	[array]$arrGrpCsvFiles = if(!$files) { 
	    Get-ChildItem $destDir -recurse *.csv | select *,@{n="type";e={$_.name.split("-")[0]}} | group type
	} else {
	    [array]$files | select *,@{n="type";e={$_.name.split("-")[0]}} | group type
	}
	$hashMetrics = [ordered]@{}
	$arrGrpCsvFiles | %{
	$arrCsvFiles = $_.group
	$typeName = $_.group[0].type
	[array]$arrMeasureResults = $arrCsvFiles | %{        
	   $csv = Import-Csv $_.pspath
	   if(!$csv) { return }
	   $name = $csv[0].name
	   [array]$arrMetricProps = $csv[0].psobject.properties | where {@("name","date") -notcontains $_.name} #| where {[system.double]::tryparse($_.value,[ref]0)}
	   $arrMetricProps | %{ $hashMetrics.$($_.name) = 1 }
	   #$csv | measure -property @($arrMetricProps | %{ $_.name}) -sum -average -maximum -ea 0 | select @{n="name";e={$name}},* 
	   $arrMetricProps.name | %{ $prop=$_;$csv.$prop | where {$_ -ge 0} | measure -sum -average -maximum -ea 0 | select @{n="name";e={$name}},@{n="property";e={$prop}},* -excludeproperty property } 
	}
	$outFile = "$($destDir)\$($typeName)-$($AssessmentName)-measuredResults.csv"
	$arrMeasureResults | group name | %{
	[array]$tmpResults = $_.group
	$hashAvgTemp = [ordered]@{name=$tmpResults[0].name;assessmentName=$AssessmentName}
	$hashSumTemp = [ordered]@{name=$tmpResults[0].name;assessmentName=$AssessmentName}
	$hashMaxTemp = [ordered]@{name=$tmpResults[0].name;assessmentName=$AssessmentName}
	$hashCountTemp = [ordered]@{name=$tmpResults[0].name;assessmentName=$AssessmentName}
	$hashAvgTemp.type = "Average"
	$hashMaxTemp.type = "Max"
	$hashSumTemp.type = "Sum"
	$hashCountTemp.type = "Count"
	$tmpResults | sort Property  | %{ 
		$hashAvgTemp.($_.Property) = $_.Average
		$hashMaxTemp.($_.Property) = $_.Maximum
		$hashSumTemp.($_.Property) = $_.Sum
		$hashCountTemp.($_.Property) = $_.Count
	} 
	[pscustomobject]$hashAvgTemp
	[pscustomobject]$hashMaxTemp
	[pscustomobject]$hashSumTemp
	[pscustomobject]$hashCountTemp

	} | select -property (@("name","type","assessmentName",($hashMetrics.keys | sort)) | %{$_})| sort type,name | export-csv $outFile -notypeinformation
	New-Object -type psobject -property @{"File"=$outFile;type="measuredResults"}

	} 	

    }
}

#Measure-vCOpsAssessmentMeasured -destDir -files (Get-Item)
Function Measure-vCOpsAssessmentMeasured {
    [CmdletBinding()]
    param($destDir=$(throw "missing -destDir"),[System.IO.FileInfo[]]$files,$assessmentName)
    Process {
	[array]$arrGrpCsvFiles = if(!$files) { 
	    Get-ChildItem $destDir -recurse *.csv | select *,@{n="type";e={$_.name.split("-")[0]}} | group type
	} else {
	    [array]$files | select *,@{n="type";e={$_.name.split("-")[0]}} | group type
	}
	$hashMetrics = [ordered]@{}
	$arrGrpCsvFiles | %{
	$arrCsvFiles = $_.group
	$typeName = $_.group[0].type
	[array]$arrResults = $arrCsvFiles | %{        
	   $csv = Import-Csv $_.pspath
	   if(!$csv) { return }
	   $name = $csv[0].name
	   [array]$arrMetricProps = $csv[0].psobject.properties | where {[system.double]::tryparse($_.value,[ref]0)}
	   $arrMetricProps | %{ $hashMetrics.$($_.name) = 1 }
	   $csv 
	}
	[array]$arrResultsGroupType = $arrResults | group type
	#[array]$arrMeasureResults = $arrResults | measure -property @($hashMetrics.keys | %{$_}) -sum -average -maximum -ea 0 | select @{n="name";e={$name}},* 
	[array]$arrMeasureResults = $arrResultsGroupType | %{
	    $parentType = $_.group[0].type
	    $_.group | measure -property @($hashMetrics.keys | %{$_}) -sum -average -maximum -ea 0 | select @{n="name";e={$name}},@{n="parentType";e={$parentType}},* 
	}

	$outFile = "$($destDir)\$($typeName)-$($AssessmentName)-consolidatedMeasuredResults.csv"
	$arrMeasureResults | group name,parentType | %{
	[array]$tmpResults = $_.group
	$hashAvgTemp = [ordered]@{assessmentName=$AssessmentName;name=$typeName;parentType=$tmpResults[0].parentType}
	$hashSumTemp = [ordered]@{assessmentName=$AssessmentName;name=$typeName;parentType=$tmpResults[0].parentType}
	$hashMaxTemp = [ordered]@{assessmentName=$AssessmentName;name=$typeName;parentType=$tmpResults[0].parentType}
	$hashCountTemp = [ordered]@{assessmentName=$AssessmentName;name=$typeName;parentType=$tmpResults[0].parentType}
	$hashAvgTemp.type = "Average"
	$hashMaxTemp.type = "Max"
	$hashSumTemp.type = "Sum"
	$hashCountTemp.type = "Count"
	$tmpResults | sort Property  | %{ 
		$hashAvgTemp.($_.Property) = $_.Average
		$hashMaxTemp.($_.Property) = $_.Maximum
		$hashSumTemp.($_.Property) = $_.Sum
		$hashCountTemp.($_.Property) = $_.Count
	} 
	[pscustomobject]$hashAvgTemp
	[pscustomobject]$hashMaxTemp
	[pscustomobject]$hashSumTemp
	[pscustomobject]$hashCountTemp

	} | select -property (@("name","assessmentName","parentType","type",($hashMetrics.keys | sort)) | %{$_})| sort parentType,type | export-csv $outFile -notypeinformation
	New-Object -type psobject -property @{"File"=$outFile;"Type"="consolidatedMeasuredResults"}

	} 	

    }
}

#New-vCOpsAssessmentsSummary -destDir $destDir_Root -files (files) -guid $guid
Function New-vCOpsAssessmentsSummary {
    [CmdletBinding()]
    param($destDir=$(throw "missing -destDir"),[System.IO.FileInfo[]]$files,$guid)
    Process {
	[array]$arrGrpCsvFiles = if(!$files) { 
	    Get-ChildItem $destDir -recurse *.csv | select *,@{n="type";e={$_.name.split("-")[0]}} | group type
	} else {
	    [array]$files | select *,@{n="type";e={$_.name.split("-")[0]}} | group type
	}
	$hashMetrics = [ordered]@{}
	
	$arrGrpCsvFiles | %{
	    $arrCsvFiles = $_.group
	    $typeName = $_.group[0].type
	    [array]$arrResults = $arrCsvFiles | %{ Import-Csv $_.pspath }
	    $outFile = "$($destDir)\$($typeName)-$($guid)-summary.csv"
	    $arrResults  | sort parentType,type,name,assessmentName | export-csv $outFile -notypeinformation
	    New-Object -type psobject -property @{"File"=$outFile;"Key"=$typeName;"Type"="vCOpsAssessmentsSummary"}
	} 
	
    }

}

#get-vmhost | select -first 1 | get-vcopsresourcemetricrecent -metrickey "badge|health"
#get-vmhost | get-vcopsresourcemetricrecent -metrickey "badge|health,badge|alert_count_critical"
#Get-vCOpsResource | where {$_.resourceName -eq "bsg04070.lss.emc.com"} | Get-vCOpsResourceMetricRecent -metrickey "badge|alert_count_critical"
function Get-vCOpsResourceMetricRecent {
[CmdletBinding()]
    param(
           [array]$metricKey,
           [Parameter(Mandatory=$true, Position=0, ValueFromPipeline=$true)]
           [psobject[]]$PsObject
     )
    Begin {
        $arrInput = @()
    }
    Process {
        $arrInput += $PsObject
    }
    End {
        if (-not $DefaultvCOPsServer) {
            Write-Error "No connection to a vCOps Server found, please use Connect-vCOpsServer to connect to a server"
            Return
        }
        $query = "action=getMetricValuesFromMemory&resources="
        [array]$arrResourceParams = $arrInput | %{
		    if($_.adapterKindKey) {
		        $adapterKindKey = $_.adapterKindKey
		        $resourceKindKey = $_.resourceKindKey -replace " ",'%20'
		        $identifiers = $_.identifiers
		        $resourceName = $_.resourceName
		    } else {
		        $adapterKindKey = "VMWARE"
		        $resourceKindKey = $_.ExtensionData.MoRef.Type
		        $identifiers = "VMEntityObjectID::$($_.ExtensionData.MoRef.Value)`$`$VMEntityVCID::$($global:DefaultVIServer.ExtensionData.Content.About.InstanceUUID)"
		        $resourceName = $_.name
		    }
            "$($resourceName),$($adapterKindKey),$($resourceKindKey),$($identifiers)"
        }
        $query = $query + ($arrResourceParams -join ";") + "&metricKeys=$($metricKey -join ",")"
        #$query = "action=getMetricValuesFromMemory&resources=bsg06081.lss.emc.com,VMWARE,HostSystem,VMEntityObjectID::host-103`$`$VMEntityVCID::4983DD9E-65E7-42CB-ACDD-B2A11D52A1C7&metricKeys=badge|health"
        #$query = "action=getMetricValuesFromMemory&resources=regex:.*,VMWARE,HostSystem&metricKeys=badge|health,badge|alert_count_critical"
        Write-Verbose $Query
        $http_request = new-object System.Net.WebClient
        $http_request.Headers.Add("Cookie",$global:jsessionid)
        $http_request.Credentials = (New-Object System.Net.NetworkCredential($DefaultvCOPsServer.Username,$DefaultvCOPsServer.Password))
        [System.Net.ServicePointManager]::ServerCertificateValidationCallback = {$true}
        $Results = $http_request.UploadString($DefaultvCOpsServer.APIURL,$Query)
        Write-Verbose $Results
        [array]$arrResults = $results -split "`n" | select-string -pattern "^{|^`t(?! )" | %{ $_.line }
        [array]$arrResources = $arrResults | select-string -pattern "^{"
        if(!$arrResources) {
            return
        }
        $i=0
        [array]$arrResourcesMetrics = $arrResources | %{
            $indexEnd = if($i -ne $arrResources.count-1) { $arrResources[$i+1].LineNumber-2 } else { $arrResults.count-1 }
            $arrResources[$i] | select *,@{n="indexMetrics";e={ ,@($_.LineNumber..$indexEnd) }}
            $i++
        } | select @{n="resourceLine";e={$_.line -replace "{|}"}},@{n="metricsLine";e={$arrResults[$_.indexMetrics] -replace "`t",""}}

        $arrResourcesMetrics | %{
            $hashResources = @{}
            $resourceName = $_.resourceLine -split "&" | %{ $hashResources.($_.split('=')[0]) = $_.split('=')[1] }
            $_.metricsLine | %{
                [array]$arrMetricSplit = $_.split(',')
                $hashResources.MetricKey = $arrMetricSplit[0]
                $hashResources.Date = ([timezone]::CurrentTimeZone.ToLocalTime(([datetime]'1/1/1970').AddSeconds([Math]::Floor($arrMetricSplit[1] /1000))))
                $hashResources.MetricValue = $arrMetricSplit[2]
                [pscustomobject]$hashResources
            }
        }

        #[array]$arrMetrics = $arrResults | select-string -pattern "^`t" 

        #MetricKey, Time, MetricValue
    }
}
