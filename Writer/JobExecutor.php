<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-02
 */

namespace Keboola\GoodDataWriterBundle\Writer;

use Keboola\GoodDataWriterBundle\Exception\JobExecutorException,
	Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Event as StorageApiEvent,
	\Keboola\StorageApi\Table as StorageApiTable;
use Keboola\GoodDataWriterBundle\GoodData\RestApi;
use Monolog\Logger;
use Symfony\Component\DependencyInjection\ContainerInterface;

class JobExecutor
{
	const APP_NAME = 'wr-gooddata';

	/**
	 * @var StorageApiClient
	 */
	protected $_sapiSharedConfig;
	/**
	 * @var Logger
	 */
	protected $_log;
	/**
	 * Current job
	 * @var
	 */
	protected $_job = null;
	/**
	 * @var StorageApiClient
	 */
	protected $_sapiClient = null;
	/**
	 * @var ContainerInterface
	 */
	protected $_container;


	/**
	 * @param StorageApiClient $sharedConfig
	 * @param Logger $log
	 * @param ContainerInterface $container
	 */
	public function __construct(StorageApiClient $sharedConfig, Logger $log, ContainerInterface $container)
	{
		$this->_sapiSharedConfig = $sharedConfig;
		$this->_log = $log;
		$this->_container = $container;
	}

	/**
	 * Job execution
	 * Performs execution of job tasks and logging
	 * @param $jobId
	 * @throws JobExecutorException
	 */
	public function runJob($jobId)
	{
		$job = $this->_job = $this->_fetchJob($jobId);

		if (!$job) {
			throw new JobExecutorException("Job $jobId not found");
		}

		try {
			$this->_sapiClient = new StorageApiClient(
				$job['token'],
				$job['sapiUrl'],
				self::APP_NAME
			);
		} catch(\Keboola\StorageApi\Exception $e) {
			throw new JobExecutorException("Invalid token for job $jobId", 0, $e);
		}
		$this->_sapiClient->setRunId($jobId);

		$time = time();
		// start work on job
		$this->_updateJob($jobId, array(
			'status' => 'processing',
			'startTime' => date('c', $time),
		));

		$result = $this->_executeJob($job);
		$jobStatus = ($result['status'] === 'error') ? StorageApiEvent::TYPE_ERROR : StorageApiEvent::TYPE_SUCCESS;

		// end work on job
		$jobInfo = array(
			'status' => $jobStatus,
			'endTime' => date('c'),
		);
		if (isset($result['response']['gdWriteStartTime'])) {
			$jobInfo['gdWriteStartTime'] = $result['response']['gdWriteStartTime'];
			unset($result['response']['gdWriteStartTime']);
		}
		$jobInfo['result'] = json_encode($result);
		$this->_updateJob($jobId, $jobInfo);
	}

	protected function _fetchJob($jobId)
	{
		$csv = $this->_sapiSharedConfig->exportTable(
			'in.c-wr-gooddata.jobs',
			null,
			array(
				'whereColumn' => 'id',
				'whereValues' => array($jobId),
			)
		);

		$jobs = StorageApiClient::parseCsv($csv, true);
		return reset($jobs);
	}

	protected function _updateJob($jobId, $fields)
	{
		$jobsTable = new StorageApiTable($this->_sapiSharedConfig, 'in.c-wr-gooddata.jobs');//@TODO
		$jobsTable->setHeader(array_merge(array('id'), array_keys($fields)));
		$jobsTable->setFromArray(array(array_merge(array($jobId), $fields)));
		$jobsTable->setPartial(true);
		$jobsTable->setIncremental(true);
		$jobsTable->save();
	}

	protected function _prepareSapiEventForJob($job)
	{
		$event = new StorageApiEvent();
		$event
			->setComponent(self::APP_NAME)
			->setConfigurationId($job['writerId'])
			->setRunId($job['id']);

		return $event;
	}

	/**
	 * Log event to client SAPI and to system log
	 * @param StorageApiEvent $event
	 */
	protected function _logEvent(StorageApiEvent $event)
	{
		$event->setParams(array_merge($event->getParams(), array(
			'jobId' => $this->_job['id'],
			'writerId' => $this->_job['writerId']
		)));
		$this->_sapiClient->createEvent($event);

		// convert priority
		switch ($event->getType()) {
			case StorageApiEvent::TYPE_ERROR:
				$priority = Logger::ERROR;
				break;
			case StorageApiEvent::TYPE_WARN:
				$priority = Logger::WARNING;
				break;
			default:
				$priority = Logger::INFO;
		}

		$this->_log($event->getMessage(), $priority, array(
			'writerId' => $event->getConfigurationId(),
			'runId' => $event->getRunId(),
			'description' => $event->getDescription(),
			'params' => $event->getParams(),
			'results' => $event->getResults(),
			'duration' => $event->getDuration(),
		));
	}

	protected function _log($message, $priority, array $data)
	{
		$this->_log->log($priority, $message, array_merge($data, array(
			'runId' => $this->_sapiClient->getRunId(),
			'token' => $this->_sapiClient->getLogData(),
			'jobId' => $this->_job['id'],
		)));
	}

	/**
	 * Excecute task and returns task execution result
	 * @param $job
	 * @throws JobExecutorException
	 * @return array
	 */
	protected function _executeJob($job)
	{
		$time = time();
		$sapiEvent = $this->_prepareSapiEventForJob($job);
		$sapiEvent->setMessage("Job $job[id] start");
		$this->_logEvent($sapiEvent);

		$result = array(
			'id' => $job['id'],
			'status' => 'ok',
		);


		try {
			$parameters = $this->_decodeParameters($job['parameters']);

			$commandName = $job['command'];
			if (!method_exists($this, $commandName)) {
				throw new JobExecutorException(sprintf('Command %s does not exist', $commandName));
			}
			$response = $this->$commandName($job, $parameters);

			$duration = time() - $time;
			$sapiEvent
				->setMessage("Job $job[id] end")
				->setDuration($duration);
			$this->_logEvent($sapiEvent);

			$result['response'] = $response;
			$result['duration'] = $duration;
			return $result;

		} catch (JobExecutorException $e) {
			$duration = $time - time();

			$sapiEvent
				->setMessage("Job $job[id] end")
				->setType(StorageApiEvent::TYPE_WARN)
				->setDescription($e->getMessage())
				->setDuration($duration);
			$this->_logEvent($sapiEvent);

			$result['status'] = 'error';
			$result['error'] = $e->getMessage();
			$result['duration'] = $duration;
			return $result;
		}
	}


	/**
	 * @param $paramsString
	 * @throws JobExecutorException
	 * @return mixed
	 */
	private function _decodeParameters($paramsString)
	{
		try {
			return \Zend_Json::decode($paramsString);
		} catch(\Zend_Json_Exception $e) {
			throw new JobExecutorException("Params decoding failed.", 0, $e);
		}
	}


	/**
	 * @param $job
	 * @param $params
	 * @throws \Keboola\GoodDataWriterBundle\Exception\JobExecutorException
	 * @return array
	 */
	public function createProject($job, $params)
	{
		if (empty($params['accessToken'])) {
			throw new JobExecutorException("Parameter accessToken is missing");
		}
		if (empty($params['projectName'])) {
			throw new JobExecutorException("Parameter projectName is missing");
		}

		$tmpDir = $this->_container->get('kernel')->getRootDir() . '/tmp';
		$configuration = new Configuration($job['writerId'], $this->_sapiClient, $tmpDir);
		$mainConfig = $this->_container->getParameter('gd_writer');


		$gdWriteStartTime = date('c');
		$backendUrl = isset($configuration->bucketInfo['gd']['backendUrl']) ? $configuration->bucketInfo['gd']['backendUrl'] : null;
		$restApi = new RestApi($backendUrl, $this->_log);
		$restApi->login($mainConfig['gd']['username'], $mainConfig['gd']['password']);

		$projectPid = $restApi->createProject($params['projectName'], $params['accessToken']);

		$username = $job['projectId'] . '-' . $job['writerId'] . '@clients.keboola.com';
		$password = md5(uniqid());
		$userUri = $restApi->createUserInDomain($mainConfig['gd']['domain'], $username, $password, 'KBC', 'Writer');

		$restApi->addUserToProject($userUri, $projectPid);

		// Save data to configuration bucket
		$this->_sapiClient->setBucketAttribute($configuration->bucketId, 'gd.pid', $projectPid);
		$this->_sapiClient->setBucketAttribute($configuration->bucketId, 'gd.username', $username);
		$this->_sapiClient->setBucketAttribute($configuration->bucketId, 'gd.password', $password, true);
		$this->_sapiClient->setBucketAttribute($configuration->bucketId, 'gd.userUri', $userUri);

		return array(
			'gdWriteStartTime' => $gdWriteStartTime
		);
	}

}