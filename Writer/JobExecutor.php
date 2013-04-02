<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-02
 */

namespace Keboola\GoodDataWriterBundle\Writer;


use Keboola\GoodDataWriterBundle\Exception\JobExecutorException;
use Symfony\Component\Security\Acl\Exception\Exception;

class JobExecutor
{

	/**
	 * @var \Keboola\StorageApi\Client
	 */
	protected $_sapiSharedConfig;

	/**
	 * @var \Monolog\Logger
	 */
	protected $_log;

	/**
	 * Current job
	 * @var
	 */
	protected $_job = null;

	/**
	 * SAPI client for current job
	 * @var \Keboola\StorageApi\Client
	 */
	protected $_sapiClient = null;


	/**
	 * @param \Keboola\StorageApi\Client $sharedConfig
	 * @param \Monolog\Logger $log
	 */
	public function __construct(\Keboola\StorageApi\Client $sharedConfig, \Monolog\Logger $log)
	{
		$this->_sapiSharedConfig = $sharedConfig;
		$this->_log = $log;
	}

	/**
	 * Job execution
	 * Performs execution of job tasks and logging
	 * @param $jobId
	 */
	public function runJob($jobId)
	{
		$job = $this->_job = $this->_fetchJob($jobId);

		if (!$job) {
			throw new JobExecutorException("Job $jobId not found");
		}

		try {
			$this->_sapiClient = new \Keboola\StorageApi\Client(
				$job['token'],
				$this->_config->storageApi->url,
				$this->_config->app->name
			);
		} catch(\Keboola\StorageApi\Exception $e) {
			throw new Service_JobExecutorException("Invalid token for job $jobId", 0, $e);
		}
		$this->_sapiClient->setRunId($jobId);

		NDebugger::timer('orchestration');
		// start work on job
		$this->_updateJob($jobId, array(
			'status' => 'processing',
			'startTime' => date('c'),
		));

		$this->_logEvent(
			$this->_prepareSapiEventForJob($job)
				->setMessage("Orchestration job $job[id] start")
		);

		// read and validate tasks
		$notificationsEmail = null;
		try {
			$configurationTable = $this->_sapiClient->getTable($job['configurationId']);
			foreach ($configurationTable['attributes'] as $attribute) {
				if ($attribute['name'] !== 'notificationsEmail') {
					continue;
				}
				$notificationsEmail = $attribute['value'];
				break;
			}

			$tasks = \Keboola\StorageApi\Client::parseCsv($this->_sapiClient->exportTable($job['configurationId']));

		} catch(\Exception $e) {
			$this->_updateJob($jobId, array(
				'status' => 'error',
				'endTime' => date('c'),
				'results' => \Zend_Json::encode(array(
					'error' => 'Error on configuration read',
					'exception' => $e->getMessage(),
				))
			));
			$event = $this->_prepareSapiEventForJob($job)
				->setType(\Keboola\StorageApi\Event::TYPE_ERROR)
				->setMessage("Orchestration job $job[id] end")
				->setDuration(NDebugger::timer('orchestration'))
				->setResults(array(
					'error' => 'Error on configuration read',
					'exception' => $e->getMessage(),
				))
				->setDescription('Error on configuration read.');
			$this->_logEvent($event);

			if ($notificationsEmail) {
				$this->_notifyUser($notificationsEmail, $event, $job);
			}
			return;
		}


		// iterate tasks - execute them and log results
		$results = array();
		foreach ($tasks as $task) {
			$result = $this->_executeJobTask($job, $task);
			$results[] = $result;
			if ($result['status'] == 'error') {
				break;
			}
		}

		$errors = array_filter($results, function($result) {
			return $result['status'] === 'error';
		});

		$jobStatus = count($errors) ? \Keboola\StorageApi\Event::TYPE_ERROR : \Keboola\StorageApi\Event::TYPE_SUCCESS;


		// end work on job
		$this->_updateJob($jobId, array(
			'status' => $jobStatus,
			'results' => \Zend_Json::encode(array(
				'tasks' => $results,
			)),
			'endTime' => date('c'),
		));
		$event = $this->_prepareSapiEventForJob($job)
			->setMessage("Orchestration job $job[id] end")
			->setDuration(NDebugger::timer('orchestration'))
			->setType($jobStatus)
			->setResults(array(
				'tasks' => $results,
			));
		$this->_logEvent($event);

		if ($notificationsEmail && $jobStatus == \Keboola\StorageApi\Event::TYPE_ERROR) {
			$this->_notifyUser($notificationsEmail, $event, $job);
		}

	}

	protected function _notifyUser($email, \Keboola\StorageApi\Event $event, $job)
	{
		$view = new \Zend_View();
		$view
			->assign(array(
				'email' => $email,
				'results' => $event->getResults(),
				'job' => $job,
			))
			->setScriptPath(APPLICATION_PATH . '/views/emails');
		$mail = new \Zend_Mail('utf8');
		$mail->addTo($email)
			->setSubject(sprintf("[KBC] %s orchestrator %s error", $job['tokenOwnerName'], $job['configurationId']))
			->setBodyHtml($view->render('orchestration-error.phtml'))
			->setFrom('support@keboola.com');

		$mail->send();
	}

	protected function _fetchJob($jobId)
	{
		$csv = $this->_sapiSharedConfig->exportTable(
			'in.c-orchestrator.jobs',
			null,
			array(
				'whereColumn' => 'id',
				'whereValues' => array($jobId),
			)
		);

		$jobs = \Keboola\StorageApi\Client::parseCsv($csv, true);
		return reset($jobs);
	}

	protected function _updateJob($jobId, $fields)
	{
		$jobsTable = new \Keboola\StorageApi\Table($this->_sapiSharedConfig, 'in.c-orchestrator.jobs');
		$jobsTable->setHeader(array_merge(array('id'), array_keys($fields)));
		$jobsTable->setFromArray(array(array_merge(array($jobId), $fields)));
		$jobsTable->setPartial(true);
		$jobsTable->setIncremental(true);
		$jobsTable->save();
	}

	protected function _prepareSapiEventForJob($job)
	{
		$event = new Keboola\StorageApi\Event();
		$event
			->setComponent($this->_config->app->name)
			->setConfigurationId($job['configurationId'])
			->setRunId($job['id']);

		return $event;
	}

	/**
	 * Log event to client SAPI and to system log
	 * @param Keboola\StorageApi\Event $event
	 */
	protected function _logEvent(\Keboola\StorageApi\Event $event)
	{
		$event->setParams(array_merge($event->getParams(), array(
			'jobId' => $this->_job['id'],
			'orchestrationId' => $this->_job['orchestrationId']
		)));
		$this->_sapiClient->createEvent($event);

		// convert priority
		switch ($event->getType()) {
			case \Keboola\StorageApi\Event::TYPE_ERROR:
				$priority = \Zend_Log::ERR;
				break;
			case \Keboola\StorageApi\Event::TYPE_WARN:
				$priority = \Zend_Log::WARN;
				break;
			default:
				$priority = \Zend_Log::INFO;
		}

		$this->_log($event->getMessage(), $priority, array(
			'configurationId' => $event->getConfigurationId(),
			'runId' => $event->getRunId(),
			'description' => $event->getDescription(),
			'params' => $event->getParams(),
			'results' => $event->getResults(),
			'duration' => $event->getDuration(),
		));
	}

	protected function _log($message, $priority, array $data)
	{
		$this->_log->log($message, $priority, array_merge($data, array(
			'runId' => $this->_sapiClient->getRunId(),
			'token' => $this->_sapiClient->getLogData(),
			'jobId' => $this->_job['id'],
		)));
	}

	/**
	 * Excecute task and returns task execution result
	 * @param Keboola\StorageApi\Client $sapiClient
	 * @param $job
	 * @param $task
	 * @return array
	 */
	protected function 	_executeJobTask($job, $task)
	{

		NDebugger::timer('task');
		$sapiEvent = $this->_prepareSapiEventForJob($job);
		$sapiEvent->setMessage("Component $task[runUrl] start");
		$this->_logEvent($sapiEvent);

		$result = array(
			'id' => $task['id'],
			'runUrl' => $task['runUrl'],
			'runParameters' => $task['runParameters'],
			'status' => 'ok',
		);


		try {
			$this->_validateJobTask($task);
			$response = $this->_taskRunRequest(
				$task['runUrl'],
				$this->_sapiClient->token,
				$this->_decodeParameters($task['runParameters']),
				$job['id']
			);

			$duration = round(NDebugger::timer('task'));
			$sapiEvent
				->setMessage("Component $task[runUrl] end")
				->setDuration($duration);
			$this->_logEvent($sapiEvent);

			$result['response'] = $response;
			$result['duration'] = $duration;
			return $result;

		} catch (Service_TaskRunException $e) {
			$duration = NDebugger::timer('task');

			$sapiEvent
				->setMessage("Component $task[runUrl] end")
				->setType(\Keboola\StorageApi\Event::TYPE_WARN)
				->setDescription($e->getMessage())
				->setDuration($duration);

			if ($e instanceof Service_TaskRunInvalidResponseException) {
				$sapiEvent->setResults($e->getResponse());
				$result['response'] = $e->getResponse();
			}
			$this->_logEvent($sapiEvent);

			$result['status'] = 'error';
			$result['error'] = $e->getMessage();
			$result['duration'] = $duration;
			return $result;
		}
	}

	/**
	 * @param $task
	 * @throws Service_TaskRunException
	 */
	private function _validateJobTask($task)
	{
		if ((isset($task['id']) && isset($task['runUrl']) && isset($task['runParameters']))) {
			return;
		}
		throw new Service_TaskRunException('Invalid task configuration format.');
	}

	/**
	 * @param $paramsString
	 * @return mixed
	 * @throws Service_TaskRunException
	 */
	private function _decodeParameters($paramsString)
	{
		try {
			return \Zend_Json::decode($paramsString);
		} catch(Zend_Json_Exception $e) {
			throw new Service_TaskRunException("Params decoding failed.", 0, $e);
		}
	}

	/**
	 * @param $url
	 * @param $token
	 * @param array $parameters
	 * @param $runId
	 * @return mixed
	 * @throws Service_TaskRunInvalidResponseException
	 * @throws Service_TaskRunException
	 */
	private function _taskRunRequest($url, $token, $parameters = array(), $runId)
	{
		try {
			$client = new \Zend_Http_Client($url, array(
				'timeout' => 5 * 60 * 60,
			));
			$client
				->setHeaders('X-StorageApi-Token', $token)
				->setHeaders('X-KBC-RunId', $runId)
				->setHeaders('X-User-Agent', $this->_config->app->name . " - JobExecutor");

			if (!empty($parameters)) {
				$client->setRawData(Zend_Json::encode($parameters), 'application/json');
			}

			$response = Zend_Json::decode($client->request('POST')->getBody());

		} catch(\Exception $e) {
			throw new Service_TaskRunException('Error on task run: ' . $e->getMessage(), 0, $e);
		}

		if (!(isset($response['status']) && $response['status'] == 'ok')) { // fuj - mel by se kontrolovat response code
			$e = new Service_TaskRunInvalidResponseException('Error response from task');
			$e->setResponse((array) $response);
			throw $e;
		}

		return $response;
	}

}