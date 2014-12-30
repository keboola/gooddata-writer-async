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
use Syrup\ComponentBundle\Monolog\Uploader\SyrupS3Uploader;

class EventLogger
{
	/**
	 * @var AppConfiguration
	 */
	private $appConfiguration;
	/**
	 * @var StorageApiClient
	 */
	private $storageApiClient;
	/**
	 * @var SyrupS3Uploader
	 */
	private $uploader;

	public function __construct(AppConfiguration $appConfiguration, StorageApiClient $storageApiClient, SyrupS3Uploader $uploader)
	{
		$this->appConfiguration = $appConfiguration;
		$this->storageApiClient = $storageApiClient;
		$this->uploader = $uploader;
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
				$params = array('details' => $this->uploader->uploadString($s3file , $jsonParams));
			}
			$event->setParams($params);
		}
		if ($duration)
			$event->setDuration($duration);
		$this->storageApiClient->createEvent($event);
	}

} 