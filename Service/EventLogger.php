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

	public function __construct(AppConfiguration $appConfiguration)
	{
		$this->appConfiguration = $appConfiguration;
		$this->storageApiClient = new StorageApiClient(array(
			'token' => $appConfiguration->sharedSapi_token,
			'url' => $appConfiguration->sharedSapi_url,
			'userAgent' => $appConfiguration->userAgent
		));
	}

	public function log($jobId, $runId, $message, $description=null, $params=array(), $startTime=null)
	{
		$event = new StorageApiEvent();
		$event
			->setMessage($message)
			->setComponent($this->appConfiguration->appName)
			->setConfigurationId($jobId)
			->setRunId($runId);
		if ($description)
			$event->setDescription($description);
		if (count($params))
			$event->setParams($params);
		if ($startTime)
			$event->setDuration(time() - $startTime);
		$this->storageApiClient->createEvent($event);
	}

} 