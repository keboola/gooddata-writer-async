<?php
/**
 * Created by PhpStorm.
 * User: JakubM
 * Date: 26.06.14
 * Time: 10:01
 */

namespace Keboola\GoodDataWriter\Service;

use Keboola\StorageApi\Client as StorageApiClient;
use Keboola\StorageApi\Event as StorageApiEvent;

class EventLogger
{
	/**
	 * @var StorageApiClient
	 */
	private $storageApiClient;
	/**
	 * @var S3Client
	 */
	private $s3Client;

	public function __construct(StorageApiClient $storageApiClient, S3Client $s3Client)
	{
		$this->storageApiClient = $storageApiClient;
		$this->s3Client = $s3Client;
	}

	public function log($jobId, $runId, $message, $params=array(), $duration=null)
	{
		$event = new StorageApiEvent();
		$event
			->setType(StorageApiEvent::TYPE_INFO)
			->setMessage($message)
			->setComponent('gooddata-writer') //@TODO load from config
			->setConfigurationId($jobId)
			->setRunId($runId);
		if (count($params)) {
			if (isset($params['password']))
				$params['password'] = '***';
			$jsonParams = json_encode($params, JSON_PRETTY_PRINT);
			if (strlen($jsonParams) > 1000) {
				$s3file = $jobId . '/' . time() . '-' . uniqid() . '.json';
				$params = array('details' => $this->s3Client->uploadString($s3file , $jsonParams, 'text/json', true));
			}
			$event->setParams($params);
		}
		if ($duration)
			$event->setDuration($duration);
		$this->storageApiClient->createEvent($event);
	}

} 