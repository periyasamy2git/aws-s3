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

	private $S3Client;
	private $Bucket;
	private $REGION;
	private $ACCESS_KEY;
	private $SECRET_KEY;

	public function connectS3Bucket($Bucket,$Region,$AccessKey,$Secret)
	{
	  try
	  {
	  	  $this->Bucket = $Bucket;
	  	  $this->REGION = $Region;
	  	  $this->ACCESS_KEY = $AccessKey;
	  	  $this->SECRET_KEY = $Secret;

	  	  $this->S3Client = S3Client::factory([
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

	function trimPath($Path)
	{
	  return ltrim(preg_replace('/(\/+)/','/',$Path),'/');
	}

	public function fileExist($FileName)
	{
	  $FileName = trimPath($FileName);	
	  try {
	  	  return $this->S3Client->doesObjectExist($this->Bucket, $FileName);	 
	    } catch (S3Exception $e) {
  	  	  return 0;
	  	}
	}

	public function getAllFiles()
	{
		try {
		  $results = $this->S3Client->getPaginator('ListObjects', [
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
		$Folder = trimPath($Folder);
		try {
		  $results = $this->S3Client->getPaginator('ListObjects', [
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
		$Folder = trimPath($Folder);
		try {
		  $objects = $this->S3Client->getIterator('ListObjects', array(
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
		return ['Size'=>$size];
	}

	public function isDirectory($Folder='')
	{
		$Folder = trimPath($Folder.'/');
		try {
	   	  $results = $this->S3Client->getPaginator('ListObjects', [
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
	   $Folder = trimPath($Folder.'/');
	   try {
		 $this->S3Client->deleteMatchingObjects($this->Bucket, $Folder);	
	   } catch (S3Exception $e) {
		 return 0;
	   }	
	}

	public function renameFile($Source,$Rename, $Action='')
	{
		$Source = trimPath($Source);	
	    $Rename = trimPath($Rename);	
	    try {
	    	$result = $this->S3Client->copyObject([
	    		'ACL' => 'private', 
	    		'Bucket' => $this->Bucket, 
	    		'Key' => $Rename,
	    		'CopySource' => "{$this->Bucket}/{$Source}"
	    	]);

	    	if($result['@metadata']['statusCode'])
	    	{
	    		if(empty($Action)) {
	    			$this->S3Client->deleteObject(['Bucket' => $this->Bucket,'Key' => $Source]);
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
		$Source = trimPath($Source);
		$NewPath = trimPath($NewPath);
		
		try {
		  $result = $this->S3Client->copyObject([
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
	}

	public function copyFolder($Source, $NewPath)
	{
	   $Source = trimPath($Source.'/');	
	   $NewPath = trimPath($NewPath.'/');	
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
	}

	public function renameFolder($Source, $Rename, $Action='')
	{
	   $Source = trimPath($Source.'/');
	   $Rename = trimPath($Rename.'/');	
	   try 
	   {
	   	if($this->getFolderFiles($Source,FALSE)) {
	   		$objects = $this->getFolderFiles($Source,TRUE);  
	   	} 

	   	foreach ($objects as $object) 
	   	{
	   		$new = str_replace($Source, $Rename, $object);
	   		$this->S3Client->copyObject([
	   			'ACL' => 'private', 
	   			'Bucket' => $this->Bucket,
	   			'Key' => $new,
	   			'CopySource' => "{$this->Bucket}/{$object}"
	   		]);
	   	}

	   	if(empty($Action)) {
	   	   deleteFolder($Source);
	   	}
	   	return 1;
	   } catch (S3Exception $e) {
	   	 return 0;
	   }			
	}

	public function uploadFile($FileName, $Source='', $ContentType='')
	{
	   $FileName = trimPath($FileName);
	   if(@filesize($Source) == 0) // For Create Folder Only
	   {
	   	try {
	   		$result = $this->S3Client->putObject([ 
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
	   	   $uploader = new MultipartUploader($this->S3Client, $Source, [
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


	public function createFolder($FileName, $Source='')
	{
		$FileName = trimPath($FileName);	
		try {
			  # actual uploading 
			$result = $this->S3Client->putObject([ 
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

	public function deleteFile($FileName)
	{  
	   $FileName = trimPath($FileName);
	   if($this->S3ClientLink)
	   {	
		   try 
		   {
		   	$result = $this->S3Client->deleteObjects([
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
	   try 
	   {
	   	  $result = $this->S3Client->deleteObjects([
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
	}

	public function downloadFolder($Folder='',$Local='')
	{
	  $Folder = trimPath($Folder);	
	  $Local = trimPath($Local);	
	  try {
   	    $this->S3Client->downloadBucket($Local,$this->Bucket,$Folder);
   	    return 1;
	  } catch (S3Exception $e) {
	  	return 0;
	  }
   	}

	public function downloadFile($file='')
	{
	  $file = trimPath($file);	
	  try
	  {
	    $this->S3Client->registerStreamWrapper(); // required
	  	  
	    ob_clean();
	    header('Content-Type: application/octet-stream');
	    header("Content-Transfer-Encoding: Binary");
	    header("Content-disposition: attachment; filename=\"" . basename($file) . "\"");
	    header('Content-Length: '.filesize("s3://{$this->Bucket}/{$file}"));

	    if($stream = fopen("s3://{$this->Bucket}/{$file}", 'r')) {
	    	while (!feof($stream)) {
	  	  	 echo fread($stream, 3050);
	  	  	}
	  	  	fclose($stream);
	  	  }
	  	  flush();
	  	} catch (S3Exception $e) {
	  	  return 'Download error occured';
	  	}
	}

	public function getFileSize($fileName)
	{
	  $fileName = trimPath($fileName);	
	  try {
		  $object = $this->S3Client->GetObject([ 
		   'Bucket' => $this->Bucket, 
		   'Key' => $fileName 
		  ]); 
		  return $object['ContentLength'];
		} catch (S3Exception $e) {
		  return 0;
		}	
	}


	public function getFileURL($fileName,$Attachment='')
	{ 
	  $fileName = trimPath($fileName);	
	  $fileType = strtolower(pathinfo($fileName, PATHINFO_EXTENSION));
	  try {
	  		$url_creator = $this->S3Client->getCommand('GetObject', [ 
	  			'Bucket' => $this->Bucket, 
	  			'Key' => $fileName 
	  		]); 
	  		$file_url = $this->S3Client->createPresignedRequest($url_creator, '+5 minutes')->getUri();
	  		if(in_array($fileType,DOCAVAILABLE) && empty($Attachment))
	  		{
	  		  return urlencode($file_url);
	  		} else {
	  		  return $file_url;
	  		}
	  	}	catch (S3Exception $e) {
	  		return 0;
	  	}
	}

	public function getFileContent($Path)
	{
	  if($this->S3ClientLink)
	  {
	  	try {
		  $object = $this->S3Client->getObject(['Bucket' => $this->Bucket, 'Key' => $Path]);
		  $Content = $object['Body']->getContents();
		  return $Content;
		} catch (S3Exception $e) {
		  return 0;
		}
	  }	else {
	  	return file_get_contents($Path);
	  } 
	}

	function transferFiles($source,$dest,$Region,$Secret)
	{

	   $client = new \Aws\S3\S3Client([
	   	'region'  => $Region,
	   	'version' => 'latest',
	   	'credentials' => [
	      'key' => $Key,
		  'secret' => $Scecret,
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


	function S3Zip($Files,$ZipName,$Path)
	{
	  try
	  {
	  	
	  	$Bucket = $setting['BUCKET_NAME'];

		$S3Client->registerStreamWrapper(); // required

		$Opt = new ArchiveOptions();
		$Opt->setContentType('application/octet-stream');
		$Opt->setEnableZip64(true); // optional - for MacOs to open archives

		$zip = new ZipStream($ZipName, $Opt);

		$path = "s3://{$Bucket}/{$Path}/".date('d-M-Y')."/{$ZipName}";
		$s3Stream = fopen($path, 'w');

		$zip->opt->setOutputStream($s3Stream); // set ZipStream's output stream to the open S3 stream

		foreach($Files as $key => $val)
		{
		  $File = 's3://'.$Bucket.'/'.$val['Path'];
		  if(file_exists($File)) {
     	    $zip->addFileFromPath($val['Name'], $File);
		  }
		}

		$zip->finish(); // send the file to S3

		return ['Name'=>$ZipName,'NoFiles'=>count($Files)];

	  } catch (InvalidParamsException $e) {
	  	return $e->getMessage();
	  }

	}


}
