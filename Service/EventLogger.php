<?php
/**
 * Created by PhpStorm.
 * User: JakubM
 * Date: 26.06.14
 * Time: 10:01
 */

namespace Keboola\GoodDataWriter\Service;

use Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Event as StorageApiEvent;
use Keboola\GoodDataWriter\Writer\AppConfiguration;

class EventLogger
{
	private $appConfiguration;
	private $storageApiClient;
	private $uploader;

	public function __construct(AppConfiguration $appConfiguration, StorageApiClient $storageApiClient, S3Client $uploader)
	{
		$this->appConfiguration = $appConfiguration;
		$this->storageApiClient = $storageApiClient;
		$this->uploader = $uploader;
	}

	public function log($jobId, $runId, $message, $description=null, $params=array(), $startTime=null, $type=StorageApiEvent::TYPE_INFO, $duration=null)
	{
		$event = new StorageApiEvent();
		$event
			->setType($type)
			->setMessage($message)
			->setComponent('gooddata-writer') //@TODO load from config
			->setConfigurationId($jobId)
			->setRunId($runId);
		if ($description)
			$event->setDescription($description);
		if (count($params)) {
			if (isset($params['password']))
				$params['password'] = '***';
			$jsonParams = json_encode($params, JSON_PRETTY_PRINT);
			if (strlen($jsonParams) > 1000) {
				$s3file = $jobId . '/' . time() . '-' . uniqid() . '.json';
				$params = array('details' => $this->uploader->url($this->uploader->uploadString($s3file , $jsonParams)));
			}
			$event->setParams($params);
		}
		if ($startTime)
			$event->setDuration(time() - $startTime);
		if ($duration)
			$event->setDuration($duration);
		$this->storageApiClient->createEvent($event);
	}

} 