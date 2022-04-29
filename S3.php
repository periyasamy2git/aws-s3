<?php 

include("./vendor/autoload.php");

use Aws\S3\S3Client;
use Aws\Common\Credentials\Credentials;
use Aws\S3\Exception\S3Exception;
use Aws\Exception\AwsException;
use Aws\S3\MultipartUploader;
use Aws\Exception\MultipartUploadException;
use Aws\S3\Transfer;
use GuzzleHttp\Promise\Promise;

use ZipStream\ZipStream;
use ZipStream\Option\Archive as ArchiveOptions;

class S3Bucket {

	private $S3;
	private $BucketName;
	private $REGION;
	private $ACCESS_KEY;
	private $SECRET_KEY;

	public function connectS3Bucket($Bucket,$Region,$AccessKey,$Secret)
	{
	  try
	  {
	  	  $this->BucketName = $Bucket;
	  	  $this->REGION = $Region;
	  	  $this->ACCESS_KEY = $AccessKey;
	  	  $this->SECRET_KEY = $Secret;

	  	  $this->S3 = S3Client::factory([
	  	 	'version'=> 'latest',
	  	 	'region' => $this->REGION,
	  	 	'credentials' => [
	  	 	  'key' => $this->ACCESS_KEY,
	  		  'secret' => $this->SECRET_KEY,
	  		]
	  	   ]);
	  } catch (Exception $e) {
	    echo '<h1>Invalid credentials</h1>'; 
	    exit;
	  }	  	 
	}

	public function fileExist($FileName)
	{
	  $FileName = trimValidatePath($FileName);	
	  try {
	  	  return $this->S3->doesObjectExist($this->Bucket, $FileName);	 
	    } catch (S3Exception $e) {
  	  	  return 0;
	  	}
	}

	public function getAllFiles()
	{
		try {
		  $results = $this->S3->getPaginator('ListObjects', [
	 	   'Bucket' => $this->Bucket
		  ]);   	 		
		  foreach ($results as $result) {
		    foreach ($result['Contents'] as $k => $object) {
			  echo '<b>'.++$k.').</b> '.$object['Key'] . '<br>';
		    }
		  }
		} catch (S3Exception $e) {
		  echo $e->getMessage();	
		}
	}

	public function getFolderFiles($Folder='')
	{
		$Folder = trimValidatePath($Folder);
		try {
		  $results = $this->S3->getPaginator('ListObjects', [
	 	   'Bucket' => $this->Bucket,
	 	   'Prefix' => $Folder
		  ]);	
   	 	  $fle = [];
   	 	  foreach ($results as $result) 
   	 	  {
   	 		foreach ($result['Contents'] as $k => $object) {
   	 	      $fle[] = $object['Key'];
   	 		}
   	 	  }
   	 	  return $fle;
   	 	} catch (S3Exception $e) {
		  return 0;	
		} 
	}

	public function getFolderSize($Folder='')
	{
		$size = 0;	
		$Folder = trimValidatePath($Folder);
		if($this->S3Link)
		{
		   try {
			  $objects = $this->S3->getIterator('ListObjects', array(
			   "Bucket" => $this->Bucket,
			   "Prefix" => $Folder
			  ));

			  $i = 0;
			  foreach ($objects as $object) {
				$size = $size + $object['Size'];
				$item = $i++;
			  }
			  return array('Size'=>$size,'Item'=>$item);
			} catch (S3Exception $e) {
			  return $e->getMessage();	
			} 
		} else {
		  if(is_dir($Folder)){
		  	foreach(new RecursiveIteratorIterator(new RecursiveDirectoryIterator($Folder)) as $file){
		  		$size += $file->getSize();
		  	}	
		  }
		  return ['Size'=>$size];
		}
	}

	public function isDirectory($Folder='')
	{
		$Folder = trimValidatePath($Folder.'/');
		try {
	   	  $results = $this->S3->getPaginator('ListObjects', [
		   'Bucket' => $this->Bucket,
		   'Prefix' => $Folder
		  ]);
				
	   	  foreach ($results as $result) {
	   		return (@count($result['Contents'])>0) ? true : false;	
	   	  }
	   	} catch (S3Exception $e) {
	   	  return false;
	   	}
	}

	public function deleteFolder($Folder)
	{
	   $Folder = trimValidatePath($Folder.'/');
	   try {
		 $this->S3->deleteMatchingObjects($this->Bucket, $Folder);	
	   } catch (S3Exception $e) {
		 return 0;
	   }	
	}

	public function renameFile($Source,$Rename, $Action='')
	{
		$Source = trimValidatePath($Source);	
	    $Rename = trimValidatePath($Rename);	
	    try {
	    	$result = $this->S3->copyObject([
	    		'ACL' => 'private', 
	    		'Bucket' => $this->Bucket, 
	    		'Key' => $Rename,
	    		'CopySource' => "{$this->Bucket}/{$Source}"
	    	]);

	    	if($result['@metadata']['statusCode'])
	    	{
	    		if(empty($Action)) {
	    			$this->S3->deleteObject(['Bucket' => $this->Bucket,'Key' => $Source]);
	    		}
	    		return 1;
	    	} else {
	    		return 0;
	    	}
	    } catch (S3Exception $e) {
	    	return 0;	
	    }
	}

	public function copyFile($Source,$NewPath)
	{
		$Source = trimValidatePath($Source);
		$NewPath = trimValidatePath($NewPath);
		if($this->S3Link)
		{
			try {
			  $result = $this->S3->copyObject([
			   'ACL' => 'private', 
			   'Bucket' => $this->Bucket, 
			   'Key' => $NewPath,
			   'CopySource' => "{$this->Bucket}/{$Source}"
			  ]); 	
			  if($result['@metadata']['statusCode'])
			  {
				return 1;	
			  } else {
				return 0;
			  }
			} catch (S3Exception $e) {
			  return 0;
			}
		} else {

 	   	   if(!is_dir(dirname($NewPath))) 
 	   	   {
			  mkdir(dirname($NewPath), 0775, true);
			  if(!is_dir(dirname($NewPath))) {
			    return 2;
			   }
			  @chmod(dirname($NewPath), 0775);
		   }

		   if(@copy($Source, $NewPath))
		   {
		     exec("chmod -R 775 '".dirname($NewPath)."/' ");
		     exec("sudo chown www-data:www-data '".dirname($NewPath)."/' ");		
		   	 return 1;
		   } else {
		   	 return 0;
		   }	 
		}
	}

	public function copyFolder($Source, $NewPath)
	{
	   $Source = trimValidatePath($Source.'/');	
	   $NewPath = trimValidatePath($NewPath.'/');	
	   if($this->S3Link)
	   {
		  try 
		  {
		  	if($this->getFolderFiles($Source,FALSE)) {
		   	  $objects = $this->getFolderFiles($Source,TRUE);  
		  	}

		   	foreach ($objects as $object)
		   	{
		   	  $new = str_replace($Source, $NewPath, $object);
		   	  $this->copyFile($object,$new);
		   	}

		   	return 1;	
		  } catch (S3Exception $e) {
		  	return 0;
		  }
	   } else {
	   	 if(!is_dir($NewPath))
	   	 {
	   	   mkdir($NewPath, 0775, true);
	   	   if(!is_dir($NewPath)) {
	   	   	return 0;
	   	   } 
	   	   chmod($NewPath, 0775);
	   	 }
	   	 exec("cp -rp '".$Source."/.' '".$NewPath."'  2>&1", $output);
	   	 if(empty($output))
	   	 {
	   	   exec("chmod -R 775 '".$NewPath."/' ");
		   exec("sudo chown www-data:www-data '".$NewPath."/' ");			
	   	   return 1;
	   	 } else {
	   	   exec("rm -rf '".$NewPath."'");
	   	   return 0;
	   	 }
	   }	
	}

	public function renameFolder($Source, $Rename, $Action='')
	{
	   $Source = trimValidatePath($Source.'/');
	   $Rename = trimValidatePath($Rename.'/');	
	   if($this->S3Link)
	   {
	   	   try {

		   	 if($this->getFolderFiles($Source,FALSE)) {
		   	   $objects = $this->getFolderFiles($Source,TRUE);  
		   	 } 

		   	 foreach ($objects as $object) 
		   	 {
		   	   $new = str_replace($Source, $Rename, $object);
		   	   $this->S3->copyObject([
		   	   	'ACL' => 'private', 
		   	   	'Bucket' => $this->Bucket,
		   	   	'Key' => $new,
		   	   	'CopySource' => "{$this->Bucket}/{$object}"
		   	   ]);
		   	 }

		   	 if(empty($Action)) {
		   	   $this->deleteFolder($Source);
		   	 }

		   	 return 1;
		   } catch (S3Exception $e) {
		   	 return 0;
		   }
	   }  else {
	   	  if(!is_dir($Rename)) {
	   	    mkdir($Rename, 0775, true);
	   	    if(!is_dir($Rename)) {
	   	   	  return 0;
	   	    }
	   	    chmod($Rename, 0775);
	   	  }
	   	  exec("cp -rp '".$Source."/.' '".$Rename."'  2>&1", $output);
	   	  if(empty($output))
	   	  {
	   	  	exec("chmod -R 775 '".$Rename."/' ");
		    exec("sudo chown www-data:www-data '".$Rename."/' ");	
	   	  	if(empty($Action)) {
	   	  	 exec("rm -rf '".$Source."'");
	   	  	}
	   	  	return 1;
	   	  } else {
	   	  	exec("rm -rf '".$Rename."'");
	   	  	return 0;
	   	  }
	   }			
	}

	public function uploadFile($FileName, $Source='', $ContentType='')
	{
	   $FileName = trimValidatePath($FileName);
	   if(@filesize($Source) == 0) // For Create Folder Only
	   {
	   	try {
	   		$result = $this->S3->putObject([ 
	   	 	 'Bucket' => $this->Bucket, 
	   		 'Key' => $FileName,
	   		 'Body' => $Source, 
	   		 'ACL' => 'private'
	   		]);	
		    # printing result 
	   		if($result['@metadata']['statusCode'])
	   		{
	   		  return 1;	
	   		} else {
	   		  return 0;
	   		}
	   	 } catch (S3Exception $e) {
	   	   return 0;
	   	 }
	   } else {

	   	 try 
	   	 {
	   	   $uploader = new MultipartUploader($this->S3, $Source, [
	   	 	 'Bucket' => $this->Bucket,
	   	 	 'Key' => $FileName,
	   	 	 'ACL' => 'private',
	   	 	 'before_initiate' => function(\Aws\Command $command) use ($ContentType) 
	   	 	 {
	   	 	   $command['ContentType'] = $ContentType;	
	   	 	 }
	   	 	]);
	   	 	$result = $uploader->upload();
	   	 	if($result['@metadata']['statusCode'])
	   	 	{
	   	 	  return 1;	
	   	 	} else {
	   	 	  return 0;
	   	 	}
	   	 } catch (MultipartUploadException $e) {
	   	 	return 0;
	   	 }
	   }
	}

	public function uploadBinaryFile($FileName, $Source='')
	{
		try {
		  # actual uploading 
		  $result = $this->S3->putObject([ 
			'Bucket' => $this->Bucket, 
			'Key' => $FileName,
			'Body' => $Source, 
			'ACL' => 'private' 
		  ]);	
		  # printing result 
		  if($result['@metadata']['statusCode'])
		  {
		    return 1;	
		  } else {
		    return 0;
		  }
		} catch (S3Exception $e) {
		  return 0;
		}
	}

	public function createFolder($FileName, $Source='')
	{
		$FileName = trimValidatePath($FileName);	
		if($this->S3Link)
		{
			try {
			  # actual uploading 
			  $result = $this->S3->putObject([ 
			    'Bucket' => $this->Bucket, 
				'Key' => $FileName,
				'Body' => $Source, 
				'ACL' => 'private' 
			  ]);
			  # printing result 
			  if($result['@metadata']['statusCode'])
			  {
				return 1;	
			  } else {
				return 0;
			  }
			} catch (S3Exception $e) {
			  return 0;
			}
		}  else {
		   if(!is_dir($FileName))
		   {
		   	 mkdir($FileName, 0775, true);
		   	 if(is_dir($FileName))
		   	 {
		   	   chmod($FileName, 0775);	
		   	   exec("chmod 775 '".$FileName."/' ");
		       exec("sudo chown www-data:www-data '".$FileName."/' ");		
		   	   return 1;
		   	 } else {
		   	   return 0;
		   	 }
		   } else {
		   	 return 1;
		   }
		}
	}

	public function deleteFile($FileName)
	{  
	   $FileName = trimValidatePath($FileName);
	   if($this->S3Link)
	   {	
		   try 
		   {
		   	$result = $this->S3->deleteObjects([
		   	'Bucket'  => $this->Bucket,
		   	'Delete' => [
		   		'Objects' => [
		   			[
		   			  'Key' => $FileName
		   			]
		   		]
		   	]
		   ]);

		   if(isset($result['Deleted']))
		   {
		    return 1;
		   } else {
		   	return 0;
		   }
		 } catch (S3Exception $e) {
		   return 0;
		 }
	  } else {
	  	if(file_exists($FileName)) 
	  	{
	  	  if(unlink($FileName))
	  	  {
	  	  	return 1;
	  	  } else {
	  	  	return 0;
	  	  }
	  	} else {
	  	  return 0;
	  	}
	  }
	}

	/**
	* @author Periyasamy S <periyasamy.s@avanzegroup.com>
	* @since 26 Feb 2021
	* @param Delete Files Path
	* @return
	* @purpose Bulk Delete files from S3
	* @thorw no exception
	*/
	public function deleteBulkFiles($Files)
	{
	   if(count($Files)<=0) { return ''; }
	   if($this->S3Link)
	   {
	   	  try 
	   	  {
	   	   	$result = $this->S3->deleteObjects([
	   	   	  'Bucket'  => $this->Bucket,
	   	   	  'Delete' => [
	   	   	    'Objects' => $Files,
	   	   	  ],
	   	   	]);

	   	   	if(isset($result['Deleted'])) {
	   	   	  return 1;
	   	   	} else {
	   	      return 0;
	   	   	}
	   	  } catch (S3Exception $e) {
	   	   	return 0;
	   	  }
	  } else {
	  	foreach ($Files as $value)
	  	{
	  	  if(@file_exists($value['Key'])) {
	  	 	@unlink($value['Key']);
	  	  }
	  	}
	  }
	}

	public function downloadFolder($Folder='',$Local='')
	{
	  $Folder = trimValidatePath($Folder);	
	  $Local = trimValidatePath($Local);	
	  try {
   	    $this->S3->downloadBucket($Local,$this->Bucket,$Folder);
   	    return 1;
	  } catch (S3Exception $e) {
	  	return 0;
	  }
   	}

	public function downloadFile($file='')
	{
	  $file = trimValidatePath($file);	
	  if($this->S3Link)
	  {
	  	try {
	  	  $this->S3->registerStreamWrapper(); // required
	  	  
	  	  ob_clean();
	  	  header('Content-Type: application/octet-stream');
	  	  header("Content-Transfer-Encoding: Binary");
	  	  header("Content-disposition: attachment; filename=\"" . basename($file) . "\"");
	  	  header('Content-Length: '.filesize("s3://{$this->Bucket}/{$file}"));

	  	  if($stream = fopen("s3://{$this->Bucket}/{$file}", 'r')) {
	  	  	while (!feof($stream)) {
	  	  	 echo fread($stream, getenv('CHUNKS_SIZE'));
	  	  	}
	  	  	fclose($stream);
	  	  }
	  	  flush();
	  	} catch (S3Exception $e) {
	  	  return 'Download error occured';
	  	}
	  } else {
	  	
	  	ob_clean();
	  	header('Content-Description: File Transfer');
	  	header('Content-Type: application/octet-stream');
	  	header("Content-Transfer-Encoding: Binary"); 
	  	header("Content-disposition: attachment; filename=\"" . basename($file) . "\"");
	  	header('Expires: 0');
	  	header('Cache-Control: must-revalidate');
	  	header('Pragma: public');
	  	header('Content-Length: '.filesize(FCPATH.$file));
	  	
	  	$download_rate = getenv('CHUNKS_SIZE');
	  	$f = fopen(FCPATH.$file, 'r');
	  	while (!feof($f)) {
		// send the file part to the web browser
	  	print fread($f, round($download_rate * 1024));
		flush();
		// sleep one second
	  	sleep(1);
	  	}

	  }
	}

	public function getFileSize($fileName)
	{
	  $fileName = trimValidatePath($fileName);	
	  if($this->S3Link)
	  {
	  	try {
		  $object = $this->S3->GetObject([ 
		   'Bucket' => $this->Bucket, 
		   'Key' => $fileName 
		  ]); 
		  return $object['ContentLength'];
		} catch (S3Exception $e) {
		  return 0;
		}
	  } else {
	  	if(file_exists($fileName))
	  	{
	  	  return @filesize($fileName);
	  	} else {
	  	  return 0;
	  	}
	  }	
	}

	/**
	* @author Ramakanta Sahoo <ramakanta.sahoo@avanzegroup.com>
	* @since 10 JAN 2022
	* @param Settings, filename
	* @return filesize
	* @purpose Get the filesize
	* @thorw no exception
	*/
	function getS3FileSize($Setting,$fileName)
	{
		$CI =& get_instance();
		$fileName = trimValidatePath($fileName);	
		if($Setting['S3_LINK'] == 1)
		{
			$this->S3Link = S3Client::factory([
				'version' => $Setting['S3_VERSION'],
				'region' => $Setting['S3_REGION'],
				'credentials' => [
					'key' => $Setting['S3_ACCESS_KEY'],
					'secret' => $Setting['S3_SECRET_KEY']
				]
			]);

			try {
			$S3object = $this->S3Link->GetObject([ 
			'Bucket' => $Setting['BUCKET_NAME'], 
			'Key' => $fileName 
			]); 
			return $S3object['ContentLength'];
			} catch (S3Exception $e) {
			return 0;
			}
		} else {
			if(file_exists($fileName))
			{
			return @filesize($fileName);
			} else {
			return 0;
			}
		}
	}

	public function getFileURL($fileName,$Attachment='')
	{ 
	  $fileName = trimValidatePath($fileName);	
	  $fileType = strtolower(pathinfo($fileName, PATHINFO_EXTENSION));
	  if($this->S3Link)
	  {
	  	try {
	  		$url_creator = $this->S3->getCommand('GetObject', [ 
	  			'Bucket' => $this->Bucket, 
	  			'Key' => $fileName 
	  		]); 
	  		$file_url = $this->S3->createPresignedRequest($url_creator, '+5 minutes')->getUri();
	  		if(in_array($fileType,DOCAVAILABLE) && empty($Attachment))
	  		{
	  		  return urlencode($file_url);
	  		} else {
	  		  return $file_url;
	  		}
	  	}	catch (S3Exception $e) {
	  		return 0;
	  	} 
	  }	else {
	  	return base_url($fileName.'?v='.rand(1,30));
	  }
	}

	public function getFileContent($Path)
	{
	  if($this->S3Link)
	  {
	  	try {
		  $object = $this->S3->getObject(['Bucket' => $this->Bucket, 'Key' => $Path]);
		  $Content = $object['Body']->getContents();
		  /*ob_clean();
		  header('Content-Type: '.$object['Content-Type']); */
		  return $Content;
		} catch (S3Exception $e) {
		  return 0;
		}
	  }	else {
	  	return file_get_contents($Path);
	  } 
	}

	/**
	* @author Periyasamy S <periyasamy.s@avanzegroup.com>
	* @since 22 Mar 2021
	* @param Source,Destination Path
	* @return Success (or) Failure 
	* @purpose Upload folder files to S3
	* @thorw no exception
	*/
	function transferSQLFilesToS3($source,$dest)
	{
	   $CI =& get_instance(); 

	   $client = new \Aws\S3\S3Client([
	   	'region'  => $CI->config->item('S3_REGION'),
	   	'version' => $CI->config->item('S3_VERSION'),
	   	'credentials' => [
	      'key' => $CI->config->item('S3_ACCESS_KEY'),
		  'secret' => $CI->config->item('S3_SECRET_KEY'),  
	     ]
	   ]);

	   $manager = new \Aws\S3\Transfer($client, $source, $dest,[
	    'before' => function (\Aws\Command $command) {
	  	  $command['concurrency'] = 20;
	   	 }
	   ]);
	   try
	   {
	   	 $promise = $manager->promise();

	   	 \GuzzleHttp\Promise\all($promise)->wait();

	   	 $promise->then(function() {
	   	   echo json_encode(['error'=>0,'msg'=>'Success'],JSON_PRETTY_PRINT); exit;
	   	 });

	   	 $promise->otherwise(function($reason) {
	   	   echo json_encode(['error'=>1,'msg'=>'Failed'],JSON_PRETTY_PRINT); exit;
	   	 });	
	   } catch (Exception $e) {
	   	 echo json_encode(['error'=>1,'msg'=>$e->getMessage()]); exit;
	   }
	}

	/**
	* @author Periyasamy S <periyasamy.s@avanzegroup.com>
	* @since 1 Jan 2020
	* @param 
	* @return
	* @purpose description
	* @thorw no exception
	*/
	function calculateDataRoomSize($Setting,$Folder)
	{
		$CI =& get_instance();

		$this->Bucket = $Setting['BUCKET_NAME'];
		$this->S3 = S3Client::factory([
		 'version'=> $Setting['S3_VERSION'],
		 'region' => $Setting['S3_REGION'],
		   'credentials' => [
			'key' => $Setting['S3_ACCESS_KEY'],
			'secret' => $Setting['S3_SECRET_KEY']
		  ]
		]);

		$objects = $this->S3->getIterator('ListObjects', array(
		 "Bucket" => $this->Bucket,
		 "Prefix" => trimValidatePath($Folder)
		));

		$i = $size = 0;
		foreach ($objects as $object) {
		 $size = $size + $object['Size'];
		 $item = $i++;
		}
		return $size;
	}

	/**
	* @author Periyasamy S <periyasamy.s@avanzegroup.com>
	* @since 15 Sep 2021
	* @param Files
	* @return Boolean
	* @purpose Edited files upload to server
	* @thorw no exception
	*/
	function upload_OrgBasedS3($Setting, $FileName, $Source, $ContentType)
	{
		$CI =& get_instance();

		$FileName = trimValidatePath($FileName);
		if($Setting['S3_LINK'] == 1)
		{
		  $Bucket = $Setting['BUCKET_NAME'];
		  $S3 = S3Client::factory([
			'version'=> $Setting['S3_VERSION'],
			'region' => $Setting['S3_REGION'],
			'credentials' => [
				'key' => $Setting['S3_ACCESS_KEY'],
				'secret' => $Setting['S3_SECRET_KEY']
			]
		  ]);

		  try 
		  {
			$uploader = new MultipartUploader($S3, $Source, [
			 'Bucket' => $Bucket,
			 'Key' => $FileName,
			 'ACL' => 'private',
			 'before_initiate' => function(\Aws\Command $command) use ($ContentType) {
				$command['ContentType'] = $ContentType;
			  }
			]);
			$result = $uploader->upload();
			if($result['@metadata']['statusCode']) {
			  return 1;
			} else {
			  return 0;
			}
		  } catch (MultipartUploadException $e) {
			return 0;
		  }
		} else {
		  if(!is_dir(dirname($FileName))) {
		  	mkdir(dirname($FileName),777,TRUE);
		  }
		  if(move_uploaded_file($Source, $FileName)) {
		  	return 1;
		  } else {
		  	return 0;
		  }	
		}
	}

	/**
	* @author Periyasamy S <periyasamy.s@avanzegroup.com>
	* @since 16 Sep 2021
	* @param Files
	* @return Boolean
	* @purpose Edited files delete
	* @thorw no exception
	*/
	function deleteEditedFile($Setting, $FileName)
	{
		$FileName = trimValidatePath($FileName);
		if($Setting['S3_LINK'] == 1)
		{
		  $Bucket = $Setting['BUCKET_NAME'];
		  $S3 = S3Client::factory([
			'version'=> $Setting['S3_VERSION'],
			'region' => $Setting['S3_REGION'],
			'credentials' => [
				'key' => $Setting['S3_ACCESS_KEY'],
				'secret' => $Setting['S3_SECRET_KEY']
			]
		  ]);

		  $S3->deleteObjects([
		   'Bucket'  => $Bucket,
		   'Delete' => [
		   	 'Objects' => [
		  		['Key' => $FileName]
		  	  ]
		    ]
		  ]);
		  return 1;
		} else {
		  if(file_exists($FileName)) {
		  	unlink($FileName);
		  	return 1;
		  } else {
		  	return 0;
		  }
		}
	}

	function moveOrgLogo($Source,$To,$Setting)
	{
		$Source = substr(trimValidatePath($Source),2);	
	    $Dest = substr(trimValidatePath($To),2);
		if($Setting['S3_LINK'] == 1)
		{
			$Bucket = $Setting['BUCKET_NAME'];
			$S3 = S3Client::factory([
			  'version'=> $Setting['S3_VERSION'],
			  'region' => $Setting['S3_REGION'],
			  'credentials' => [
				'key' => $Setting['S3_ACCESS_KEY'],
				'secret' => $Setting['S3_SECRET_KEY']
			   ]
			]);

			try {
				$result = $S3->copyObject([
				  'ACL' => 'private', 
				  'Bucket' => $Bucket,
				  'Key' => $Dest,
				  'CopySource' => trimValidatePath($Bucket.'/'.$Source)
				]);

				if($result['@metadata']['statusCode'])
				{
				  $S3->deleteObject(['Bucket' => $Bucket,'Key' => $Source]);
				  return 1;
				} else {
				  return 0;
				}
			} catch (S3Exception $e) {
			  return 0;	
			}
		} else {
		  if(!is_dir(dirname($Rename))) {
		  	mkdir(dirname($Rename), 0775, true);
		  	if(!is_dir(dirname($Rename))) {
		  	  return 0;
		  	}
		   	chmod(dirname($Rename), 0775);
		  }	
		  exec("cp -p '".$Source."' '".$Rename."' 2>&1", $output);	
		  if(empty($output)) 
		  {
		    exec("chmod -R 775 '".dirname($Rename)."/' ");
		    exec("sudo chown www-data:www-data '".dirname($Rename)."/' ");
			return 1;
		  } else {
		  	return 0;
		  }
		}
	}

	function S3Zip($setting,$Objects,$ZipName,$OrgUID)
	{
	  $CI =& get_instance();
	  try
	  {
	  	$S3 = S3Client::factory([
	  	 'version'=> $setting['S3_VERSION'],
	  	 'region' => $setting['S3_REGION'],
	  	 'credentials' => [
	  	  'key' => $setting['S3_ACCESS_KEY'],
	  	  'secret' => $setting['S3_SECRET_KEY'] 
	  	 ]
	  	]);

	  	$Bucket = $setting['BUCKET_NAME'];

		$S3->registerStreamWrapper(); // required

		$Opt = new ArchiveOptions();
		$Opt->setContentType('application/octet-stream');
		$Opt->setEnableZip64(true); // optional - for MacOs to open archives

		$zip = new ZipStream($ZipName, $Opt);

		$path = "s3://{$Bucket}/BACKUP/ORG-{$OrgUID}/".date('d-M-Y')."/{$ZipName}";
		$s3Stream = fopen($path, 'w');

		$zip->opt->setOutputStream($s3Stream); // set ZipStream's output stream to the open S3 stream

		foreach($Objects as $key => $val)
		{
		  $File = 's3://'.$Bucket.'/'.$val['Path'];
		  if(file_exists($File)) {
     	    $zip->addFileFromPath($val['Name'], $File);
		  }
		}

		$zip->finish(); // send the file to S3

		return ['Name'=>$ZipName,'NoFiles'=>count($Objects)];

	  } catch (InvalidParamsException $e) {
	  	return $e->getMessage();
	  }

	}

	/**
	* @author Periyasamy S <periyasamy.s@avanzegroup.com>
	* @since 24 Nov 2021
	* @param Setting,Object,FileName,Path
	* @return Array (or) Error
	* @purpose Zip the files in S3 and Store in Download
	* @thorw no exception
	*/
	function S3BackgroundDownload($setting,$Objects,$ZipName,$StorePath)
	{
	  $CI =& get_instance();
	  try
	  {
	  	$S3 = S3Client::factory([
	  	 'version'=> $setting['S3_VERSION'],
	  	 'region' => $setting['S3_REGION'],
	  	 'credentials' => [
	  	  'key' => $setting['S3_ACCESS_KEY'],
	  	  'secret' => $setting['S3_SECRET_KEY'] 
	  	 ]
	  	]);

	  	$Bucket = $setting['BUCKET_NAME'];

		$S3->registerStreamWrapper(); // required

		$Opt = new ArchiveOptions();
		$Opt->setContentType('application/octet-stream');
		$Opt->setEnableZip64(true); // optional - for MacOs to open archives

		$zip = new ZipStream($ZipName, $Opt);

		$path = "s3://{$Bucket}/{$StorePath}{$ZipName}";
		$s3Stream = fopen($path, 'w');

		$zip->opt->setOutputStream($s3Stream); // set ZipStream's output stream to the open S3 stream

		foreach($Objects as $key => $val)
		{
		  $File = 's3://'.$Bucket.'/'.$val['Path'];
		  if(file_exists($File)) {
     	    $zip->addFileFromPath($val['Name'], $File);
		  }
		}

		$zip->finish(); // send the file to S3

		return ['Name'=>$ZipName,'NoFiles'=>count($Objects)];

	  } catch (InvalidParamsException $e) {
	  	return $e->getMessage();
	  }
	}

	/**
	* @author Periyasamy S <periyasamy.s@avanzegroup.com>
	* @since 24 Nov 2021
	* @param Setting,FileName
	* @return Array (or) Error
	* @purpose Delete downloads
	* @thorw no exception
	*/
	function S3DeleteDownload($setting,$FileName)
	{
	  $CI =& get_instance();
	  try
	  {
	  	$S3 = S3Client::factory([
	  	 'version'=> $setting['S3_VERSION'],
	  	 'region' => $setting['S3_REGION'],
	  	 'credentials' => [
	  	  'key' => $setting['S3_ACCESS_KEY'],
	  	  'secret' => $setting['S3_SECRET_KEY'] 
	  	 ]
	  	]);
	  	$Bucket = $setting['BUCKET_NAME'];

		$result = $S3->deleteObjects([
		 'Bucket'  => $Bucket,
		 'Delete' => [
		   'Objects' => [
		      ['Key' => $FileName]
		   	]
		  ]
		]);

		if(isset($result['Deleted'])) {
		  return 1;
		} else {
		  return 0;
		}
	  } catch (S3Exception $e) {
	  	return 0;
	  }
	}

}
