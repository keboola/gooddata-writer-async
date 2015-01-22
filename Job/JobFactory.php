<?php
/**
 * @package gooddata-writer
 * @copyright 2015 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\JobProcessException;
use Keboola\GoodDataWriter\Service\EventLogger;
use Keboola\GoodDataWriter\Service\Queue;
use Keboola\GoodDataWriter\Service\S3Client;
use Keboola\GoodDataWriter\Writer\Configuration;
use Keboola\GoodDataWriter\Writer\SharedStorage;
use Keboola\StorageApi\Client;
use Keboola\Temp\Temp;
use Monolog\Logger;
use Symfony\Bundle\FrameworkBundle\Translation\Translator;
use Syrup\ComponentBundle\Monolog\Uploader\SyrupS3Uploader;

class JobFactory
{
	private $gdConfig;
	/**
	 * @var SharedStorage
	 */
	private $sharedStorage;
	/**
	 * @var Configuration
	 */
	private $configuration;
	/**
	 * @var Client
	 */
	private $storageApiClient;
	private $scriptsPath;
	/**
	 * @var EventLogger
	 */
	private $eventLogger;
	/**
	 * @var Translator
	 */
	private $translator;
	/**
	 * @var Temp
	 */
	private $temp;
	/**
	 * @var Logger
	 */
	private $logger;
	/**
	 * @var S3Client
	 */
	private $s3Client;
	/**
	 * @var SyrupS3Uploader
	 */
	private $s3Uploader;
	/**
	 * @var Queue
	 */
	private $queue;

	public function __construct($gdConfig, $sharedStorage, $configuration, $storageApiClient, $scriptsPath, $eventLogger, $translator, $temp, $logger, $s3Client, $s3Uploader, $queue)
	{
		$this->gdConfig = $gdConfig;
		$this->sharedStorage = $sharedStorage;
		$this->configuration = $configuration;
		$this->storageApiClient = $storageApiClient;
		$this->scriptsPath = $scriptsPath;
		$this->eventLogger = $eventLogger;
		$this->translator = $translator;
		$this->temp = $temp;
		$this->logger = $logger;
		$this->s3Client = $s3Client;
		$this->s3Uploader = $s3Uploader;
		$this->queue = $queue;
	}

	public function getJobClass($jobName)
	{
		$commandName = ucfirst($jobName);
		$commandClass = 'Keboola\GoodDataWriter\Job\\' . $commandName;
		if (!class_exists($commandClass)) {
			throw new JobProcessException($this->translator->trans('job_executor.command_not_found %1', array('%1' => $commandName)));
		}

		/**
		 * @var \Keboola\GoodDataWriter\Job\AbstractJob $command
		 */
		$command = new $commandClass($this->configuration, $this->gdConfig, $this->sharedStorage, $this->storageApiClient);
		$command->setScriptsPath($this->scriptsPath);
		$command->setEventLogger($this->eventLogger);
		$command->setTranslator($this->translator);
		$command->setTemp($this->temp); //For csv handler
		$command->setLogger($this->logger); //For csv handler
		$command->setS3Uploader($this->s3Uploader);

		$command->setFactory($this);

		return $command;
	}

	public function enqueueJob($batchId, $delay=0)
	{
		$this->queue->enqueue(array(
			'projectId' => $this->configuration->projectId,
			'writerId' => $this->configuration->writerId,
			'batchId' => $batchId
		), $delay);
	}

	public function createJob($jobName, $params, $batchId=null, $queue=SharedStorage::PRIMARY_QUEUE, $others=array())
	{
		$jobId = $this->storageApiClient->generateId();
		$tokenData = $this->storageApiClient->getLogData();
		$jobData = array(
			'projectId' => $this->configuration->projectId,
			'writerId' => $this->configuration->writerId,
			'command' => $jobName,
			'batchId' => $batchId? $batchId : $jobId,
			'parameters' => $params,
			'runId' => $this->storageApiClient->getRunId() ?: $jobId,
			'token' => $this->storageApiClient->token,
			'tokenId' => $tokenData['id'],
			'tokenDesc' => $tokenData['description']
		);
		if (count($others)) {
			$jobData = array_merge($jobData, $others);
		}

		$jobData = $this->sharedStorage->createJob($jobId, $this->configuration->projectId, $this->configuration->writerId, $jobData, $queue);

		array_walk($params, function(&$val, $key) {
			if ($key == 'password') $val = '***';
		});
		$this->eventLogger->log($jobData['id'], $this->storageApiClient->getRunId(),
			$this->translator->trans($this->translator->trans('log.job.created')), array(
				'projectId' => $this->configuration->projectId,
				'writerId' => $this->configuration->writerId,
				'runId' => $this->storageApiClient->getRunId() ?: $jobId,
				'command' => $jobName,
				'params' => $params
			));

		return $jobData;
	}

	public function saveDefinition($jobId, $definition)
	{
		$definitionUrl = $this->s3Client->uploadString(sprintf('%s/definition.json', $jobId), json_encode($definition));
		$this->sharedStorage->saveJob($jobId, array('definition' => $definitionUrl));
	}

	public function getDefinition($definitionFile)
	{
		$definition = $this->s3Client->downloadFile($definitionFile);
		$definition = json_decode($definition, true);
		if (!$definition) {
			throw new \Exception($this->translator->trans('error.s3_download_fail') . ': ' . $definitionFile);
		}
		return $definition;
	}

	public function createBatchId()
	{
		return $this->storageApiClient->generateId();
	}

}