<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-02
 */

namespace Keboola\GoodDataWriter\Writer;

use Keboola\GoodDataWriter\Exception\JobExecutorException,
	Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Event as StorageApiEvent,
	\Keboola\StorageApi\Table as StorageApiTable;
use Keboola\GoodDataWriter\Exception\RestApiException;
use Keboola\GoodDataWriter\Exception\UnauthorizedException;
use Keboola\GoodDataWriter\GoodData\CLToolApi;
use Keboola\GoodDataWriter\GoodData\CLToolApiErrorException;
use Keboola\GoodDataWriter\GoodData\RestApi;
use Monolog\Logger;
use Symfony\Component\DependencyInjection\ContainerInterface;

class JobExecutor
{
	const APP_NAME = 'wr-gooddata';

	const JOBS_TABLE_ID = 'in.c-wr-gooddata.jobs';
	const PROJECTS_TABLE_ID = 'in.c-wr-gooddata.projects';
	const USERS_TABLE_ID = 'in.c-wr-gooddata.users';
	const PROJECTS_TO_DELETE_TABLE_ID = 'in.c-wr-gooddata.projects_to_delete';
	const USERS_TO_DELETE_TABLE_ID = 'in.c-wr-gooddata.users_to_delete';

	protected $_roles = array(
		'admin' => 'adminRole',
		'editor' => 'editorRole',
		'readOnly' => 'readOnlyUserRole',
		'dashboardOnly' => 'dashboardOnlyRole'
	);

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
		if (isset($result['response']['log'])) {
			$jobInfo['log'] = $result['response']['log'];
			unset($result['response']['log']);
		}
		$jobInfo['result'] = json_encode($result);
		$this->_updateJob($jobId, $jobInfo);
	}

	/**
	 * @TODO duplicate in JobManager
	 * @param $jobId
	 * @return mixed
	 */
	protected function _fetchJob($jobId)
	{
		$csv = $this->_sapiSharedConfig->exportTable(
			self::JOBS_TABLE_ID,
			null,
			array(
				'whereColumn' => 'id',
				'whereValues' => array($jobId),
			)
		);

		$jobs = StorageApiClient::parseCsv($csv, true);
		return reset($jobs);
	}

	/**
	 * @TODO duplicate in JobManager
	 * @param $jobId
	 * @param $fields
	 */
	protected function _updateJob($jobId, $fields)
	{
		$jobsTable = new StorageApiTable($this->_sapiSharedConfig, self::JOBS_TABLE_ID);
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

			$duration = $time - time();
			$sapiEvent
				->setMessage("Job $job[id] end")
				->setDuration($duration);
			$this->_logEvent($sapiEvent);

			if (isset($response['status'])) {
				$result['status'] = $response['status'];
				unset($response['status']);
			}
			if (isset($response['error'])) {
				$result['error'] = $response['error'];
				unset($response['error']);
			}

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

	public function addProjectToConfiguration($pid, $accessToken, $backendUrl, $job)
	{
		$data = array(
			'pid' => $pid,
			'projectId' => $job['projectId'],
			'writerId' => $job['writerId'],
			'backendUrl' => $backendUrl,
			'accessToken' => $accessToken,
			'createdTime' => date('c')
		);
		$table = new StorageApiTable($this->_sapiSharedConfig, self::PROJECTS_TABLE_ID);
		$table->setHeader(array_keys($data));
		$table->setFromArray(array($data));
		$table->setIncremental(true);
		$table->save();
	}

	public function addUserToConfiguration($uri, $email, $job)
	{
		$data = array(
			'uri' => $uri,
			'projectId' => $job['projectId'],
			'writerId' => $job['writerId'],
			'email' => $email,
			'createdTime' => date('c')
		);
		$table = new StorageApiTable($this->_sapiSharedConfig, self::USERS_TABLE_ID);
		$table->setHeader(array_keys($data));
		$table->setFromArray(array($data));
		$table->setIncremental(true);
		$table->save();
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



	protected function _prepareResult($jobId, $data = array(), $callsLog = null)
	{
		$logUploader = $this->_container->get('syrup.monolog.s3_uploader');
		$logUrl = null;
		if ($callsLog) {
			$logUrl = $logUploader->uploadString('calls-' . $jobId, $callsLog);
		}

		if ($logUrl) {
			$data['log'] = $logUrl;
		}

		return $data;
	}


	/**
	 * @param $job
	 * @param $params
	 * @throws \Keboola\GoodDataWriter\Exception\JobExecutorException
	 * @return array
	 */
	public function createWriter($job, $params)
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
		$mainConfig = empty($params['dev']) ? $mainConfig['gd']['prod'] : $mainConfig['gd']['dev'];


		$gdWriteStartTime = date('c');
		$username = sprintf($mainConfig['user_email'], $job['projectId'], $job['writerId']);
		$password = md5(uniqid());
		$backendUrl = isset($configuration->bucketInfo['gd']['backendUrl']) ? $configuration->bucketInfo['gd']['backendUrl'] : null;


		$restApi = new RestApi($backendUrl, $this->_log);
		$restApi->login($mainConfig['username'], $mainConfig['password']);
		$projectPid = $restApi->createProject($params['projectName'], $params['accessToken']);
		$userUri = $restApi->createUserInDomain($mainConfig['domain'], $username, $password, 'KBC', 'Writer', $mainConfig['sso_provider']);
		$restApi->addUserToProject($userUri, $projectPid);

		// Save data to configuration bucket
		$this->_sapiClient->setBucketAttribute($configuration->bucketId, 'gd.pid', $projectPid);
		$this->_sapiClient->setBucketAttribute($configuration->bucketId, 'gd.username', $username);
		$this->_sapiClient->setBucketAttribute($configuration->bucketId, 'gd.password', $password, true);
		$this->_sapiClient->setBucketAttribute($configuration->bucketId, 'gd.userUri', $userUri);

		$this->addProjectToConfiguration($projectPid, $params['accessToken'], $backendUrl, $job);
		$this->addUserToConfiguration($userUri, $username, $job);


		return $this->_prepareResult($job['id'], array('pid' => $projectPid, 'gdWriteStartTime' => $gdWriteStartTime), $restApi->callsLog());
	}


	/**
	 * @param $job
	 * @param $params
	 */
	public function dropWriter($job, $params)
	{
		$tmpDir = $this->_container->get('kernel')->getRootDir() . '/tmp';
		$configuration = new Configuration($job['writerId'], $this->_sapiClient, $tmpDir);

		$configuration->prepareProjects();
		$configuration->prepareUsers();

		$firstLine = true;
		foreach ($configuration->projectsCsv as $project) {
			if (!$firstLine)
				$this->_enqueueProjectToDelete($job['projectId'], $job['writerId'], $project[0], empty($params['dev']));
			$firstLine = false;
		}
		$firstLine = true;
		foreach ($configuration->usersCsv as $user) {
			if (!$firstLine)
				$this->_enqueueUserToDelete($job['projectId'], $job['writerId'], $user[1], $user[0], empty($params['dev']));
			$firstLine = false;
		}

		if (isset($configuration->bucketInfo['gd']['pid'])) {
			$this->_enqueueProjectToDelete($job['projectId'], $job['writerId'], $configuration->bucketInfo['gd']['pid'], empty($params['dev']));
		}
		if (isset($configuration->bucketInfo['gd']['userUri']) && isset($configuration->bucketInfo['gd']['username'])) {
			$this->_enqueueUserToDelete($job['projectId'], $job['writerId'], $configuration->bucketInfo['gd']['userUri'],
				$configuration->bucketInfo['gd']['username'], empty($params['dev']));
		}

		foreach ($this->_sapiClient->listTables($configuration->bucketId) as $table) {
			$this->_sapiClient->dropTable($table['id']);
		}
		$this->_sapiClient->dropBucket($configuration->bucketId);
	}


	public function cloneProject($job, $params)
	{
		if (empty($params['accessToken'])) {
			throw new JobExecutorException("Parameter accessToken is missing");
		}
		if (empty($params['projectName'])) {
			throw new JobExecutorException("Parameter projectName is missing");
		}
		if (empty($params['pidSource'])) {
			throw new JobExecutorException("Parameter pidSource is missing");
		}

		$tmpDir = $this->_container->get('kernel')->getRootDir() . '/tmp';
		$configuration = new Configuration($job['writerId'], $this->_sapiClient, $tmpDir);
		$mainConfig = $this->_container->getParameter('gd_writer');
		$mainConfig = empty($params['dev']) ? $mainConfig['gd']['prod'] : $mainConfig['gd']['dev'];

		if (empty($configuration->bucketInfo['gd']['userUri']) || empty($configuration->bucketInfo['gd']['pid'])
			|| empty($configuration->bucketInfo['gd']['username']) || empty($configuration->bucketInfo['gd']['password'])) {
			throw new JobExecutorException("Bucket config is not complete");
		}


		$gdWriteStartTime = date('c');
		$backendUrl = isset($configuration->bucketInfo['gd']['backendUrl']) ? $configuration->bucketInfo['gd']['backendUrl'] : null;


		$restApi = new RestApi($backendUrl, $this->_log);
		try {
			// Check access to source project
			$restApi->login($configuration->bucketInfo['gd']['username'], $configuration->bucketInfo['gd']['password']);
			$restApi->getProject($configuration->bucketInfo['gd']['pid']);

			$restApi->login($mainConfig['username'], $mainConfig['password']);
			// Get user uri if not set
			if (empty($configuration->bucketInfo['gd']['userUri'])) {
				$userUri = $restApi->userUri($configuration->bucketInfo['gd']['username'], $mainConfig['domain']);
				$this->_sapiClient->setBucketAttribute($configuration->bucketId, 'gd.userUri', $userUri);
				$configuration->bucketInfo['gd']['userUri'] = $userUri;
			}
			$projectPid = $restApi->createProject($params['projectName'], $params['accessToken']);
			$restApi->cloneProject($configuration->bucketInfo['gd']['pid'], $projectPid);
			$restApi->addUserToProject($configuration->bucketInfo['gd']['userUri'], $projectPid);

			$configuration->addProjectToConfiguration($projectPid);
			$this->addProjectToConfiguration($projectPid, $params['accessToken'], $backendUrl, $job);


			return $this->_prepareResult($job['id'], array('pid' => $projectPid, 'gdWriteStartTime' => $gdWriteStartTime), $restApi->callsLog());


		} catch (UnauthorizedException $e) {
			throw new JobExecutorException('Login failed');
		} catch (RestApiException $e) {
			return $this->_prepareResult($job['id'], array('status' => 'error', 'error' => $e->getMessage(), 'gdWriteStartTime' => $gdWriteStartTime), $restApi->callsLog());
		}
	}


	public function addUserToProject($job, $params)
	{
		if (empty($params['pid'])) {
			throw new JobExecutorException("Parameter 'pid' is missing");
		}
		if (empty($params['email'])) {
			throw new JobExecutorException("Parameter 'email' is missing");
		}
		if (empty($params['role'])) {
			throw new JobExecutorException("Parameter 'role' is missing");
		}

		$tmpDir = $this->_container->get('kernel')->getRootDir() . '/tmp';
		$configuration = new Configuration($job['writerId'], $this->_sapiClient, $tmpDir);
		$mainConfig = $this->_container->getParameter('gd_writer');
		$mainConfig = empty($params['dev']) ? $mainConfig['gd']['prod'] : $mainConfig['gd']['dev'];

		$gdWriteStartTime = date('c');
		$backendUrl = isset($configuration->bucketInfo['gd']['backendUrl']) ? $configuration->bucketInfo['gd']['backendUrl'] : null;

		$restApi = new RestApi($backendUrl, $this->_log);
		try {
			// Get user uri
			$restApi->login($mainConfig['username'], $mainConfig['password']);
			$userUri = $restApi->userUri($params['email'], $mainConfig['domain']);
			if (!$userUri) {
				throw new JobExecutorException(sprintf("User '%s' does not exist in domain", $params['email']));
			}

			$restApi->addUserToProject($userUri, $params['pid'], $this->_roles[$params['role']]);

			$configuration->addProjectUserToConfiguration($params['pid'], $params['email'], $params['role']);


			return $this->_prepareResult($job['id'], array('gdWriteStartTime' => $gdWriteStartTime), $restApi->callsLog());

		} catch (UnauthorizedException $e) {
			throw new JobExecutorException('Login failed');
		} catch (RestApiException $e) {
			return $this->_prepareResult($job['id'], array('status' => 'error', 'error' => $e->getMessage(), 'gdWriteStartTime' => $gdWriteStartTime), $restApi->callsLog());
		}
	}


	public function inviteUserToProject($job, $params)
	{
		if (empty($params['email'])) {
			throw new JobExecutorException("Parameter 'email' is missing");
		}
		if (empty($params['role'])) {
			throw new JobExecutorException("Parameter 'role' is missing");
		}

		$tmpDir = $this->_container->get('kernel')->getRootDir() . '/tmp';
		$configuration = new Configuration($job['writerId'], $this->_sapiClient, $tmpDir);

		if (empty($configuration->bucketInfo['gd']['userUri']) || empty($configuration->bucketInfo['gd']['pid'])
			|| empty($configuration->bucketInfo['gd']['username']) || empty($configuration->bucketInfo['gd']['password'])) {
			throw new JobExecutorException("Bucket configuration is not complete");
		}

		if (empty($params['pid'])) {
			if (empty($configuration->bucketInfo['gd']['pid'])) {
				throw new JobExecutorException("Parameter 'pid' is missing and writer does not have primary project");
			}
			$params['pid'] = $configuration->bucketInfo['gd']['pid'];
		}


		$gdWriteStartTime = date('c');
		$backendUrl = isset($configuration->bucketInfo['gd']['backendUrl']) ? $configuration->bucketInfo['gd']['backendUrl'] : null;

		$restApi = new RestApi($backendUrl, $this->_log);
		try {
			$restApi->login($configuration->bucketInfo['gd']['username'], $configuration->bucketInfo['gd']['password']);
			$restApi->inviteUserToProject($params['email'], $params['pid'], $this->_roles[$params['role']]);

			$configuration->addProjectUserToConfiguration($params['pid'], $params['email'], $params['role']);

			return $this->_prepareResult($job['id'], array('gdWriteStartTime' => $gdWriteStartTime), $restApi->callsLog());

		} catch (UnauthorizedException $e) {
			throw new JobExecutorException('Login failed');
		} catch (RestApiException $e) {
			return $this->_prepareResult($job['id'], array('status' => 'error', 'error' => $e->getMessage(), 'gdWriteStartTime' => $gdWriteStartTime), $restApi->callsLog());
		}
	}


	public function createUser($job, $params)
	{
		if (empty($params['email'])) {
			throw new JobExecutorException("Parameter 'email' is missing");
		}
		if (empty($params['password'])) {
			throw new JobExecutorException("Parameter 'password' is missing");
		}
		if (empty($params['firstName'])) {
			throw new JobExecutorException("Parameter 'firstName' is missing");
		}
		if (empty($params['lastName'])) {
			throw new JobExecutorException("Parameter 'lastName' is missing");
		}

		$tmpDir = $this->_container->get('kernel')->getRootDir() . '/tmp';
		$configuration = new Configuration($job['writerId'], $this->_sapiClient, $tmpDir);
		$mainConfig = $this->_container->getParameter('gd_writer');
		$mainConfig = empty($params['dev']) ? $mainConfig['gd']['prod'] : $mainConfig['gd']['dev'];


		$gdWriteStartTime = date('c');
		$backendUrl = isset($configuration->bucketInfo['gd']['backendUrl']) ? $configuration->bucketInfo['gd']['backendUrl'] : null;

		$restApi = new RestApi($backendUrl, $this->_log);
		try {
			$restApi->login($mainConfig['username'], $mainConfig['password']);
			$userUri = $restApi->createUserInDomain($mainConfig['domain'], $params['email'], $params['password'], $params['firstName'], $params['lastName'], $mainConfig['sso_provider']);

			$configuration->addUserToConfiguration($params['email'], $userUri);
			$this->addUserToConfiguration($userUri, $params['email'], $job);


			return $this->_prepareResult($job['id'], array('uri' => $userUri, 'gdWriteStartTime' => $gdWriteStartTime), $restApi->callsLog());

		} catch (UnauthorizedException $e) {
			throw new JobExecutorException('Login failed');
		} catch (RestApiException $e) {
			return $this->_prepareResult($job['id'], array('status' => 'error', 'error' => $e->getMessage(), 'gdWriteStartTime' => $gdWriteStartTime), $restApi->callsLog());
		}
	}

	public function createDate($job, $params)
	{
		if (empty($job['dataset'])) {
			throw new JobExecutorException("Parameter 'dataset' is missing");
		}
		if (empty($params['includeTime'])) {
			throw new JobExecutorException("Parameter 'includeTime' is missing");
		}
		if (empty($job['pid'])) {
			throw new JobExecutorException("Parameter 'pid' is missing");
		}

		$tmpDir = $this->_container->get('kernel')->getRootDir() . '/tmp';
		$configuration = new Configuration($job['writerId'], $this->_sapiClient, $tmpDir);

		if (empty($configuration->bucketInfo['gd']['userUri']) || empty($configuration->bucketInfo['gd']['pid'])
			|| empty($configuration->bucketInfo['gd']['username']) || empty($configuration->bucketInfo['gd']['password'])) {
			throw new JobExecutorException("Bucket configuration is not complete");
		}


		$gdWriteStartTime = date('c');

		$clToolApi = new CLToolApi($this->_log);
		if (isset($configuration->bucketInfo['gd']['backendUrl'])) {
			$clToolApi->setBackendUrl($configuration->bucketInfo['gd']['backendUrl']);
		}
		$clToolApi->setCredentials($configuration->bucketInfo['gd']['username'], $configuration->bucketInfo['gd']['password']);
		$clToolApi->tmpDir = $tmpDir;
		$clToolApi->clToolPath = $this->_container->get('kernel')->getRootDir . '/vendor/keboola/gooddata-writer/GoodData/gdi.sh';
		$clToolApi->jobId = $job['id'];
		$clToolApi->s3uploader = $this->_container->get('syrup.monolog.s3_uploader');

		try {

			$clToolApi->createDate($job['pid'], $job['dataset'], $params['includeTime']);

			return $this->_prepareResult($job['id'], array('debug' => $clToolApi->debugLogUrl, 'gdWriteStartTime' => $gdWriteStartTime), $clToolApi->output);

		} catch (CLToolApiErrorException $e) {
			return $this->_prepareResult($job['id'], array('status' => 'error', 'error' => $e->getMessage(),
				'debug' => $clToolApi->debugLogUrl, 'gdWriteStartTime' => $gdWriteStartTime), $clToolApi->output);
		}
	}

	public function createDataset($job, $params)
	{

	}

	public function updateDataset($job, $params)
	{

	}

	public function loadData($job, $params)
	{

	}

	public function executeReports($job, $params)
	{

	}


	/**
	 * @param $projectId
	 * @param $writerId
	 * @param $pid
	 * @param int $dev
	 */
	private function _enqueueProjectToDelete($projectId, $writerId, $pid, $dev = 0)
	{
		$data = array(
			'pid' => $pid,
			'projectId' => $projectId,
			'writerId' => $writerId,
			'deleteDate' => date('c', strtotime('+30 days')),
			'dev' => $dev
		);
		$table = new StorageApiTable($this->_sapiSharedConfig, self::PROJECTS_TO_DELETE_TABLE_ID, null, 'pid');
		$table->setHeader(array_keys($data));
		$table->setFromArray(array($data));
		$table->setPartial(true);
		$table->setIncremental(true);
		$table->save();
	}

	/**
	 * @param $projectId
	 * @param $writerId
	 * @param $uri
	 * @param $email
	 * @param int $dev
	 */
	private function _enqueueUserToDelete($projectId, $writerId, $uri, $email, $dev = 0)
	{
		$data = array(
			'uri' => $uri,
			'projectId' => $projectId,
			'writerId' => $writerId,
			'email' => $email,
			'deleteDate' => date('c', strtotime('+30 days')),
			'dev' => $dev
		);
		$table = new StorageApiTable($this->_sapiSharedConfig, self::USERS_TO_DELETE_TABLE_ID, null, 'uri');
		$table->setHeader(array_keys($data));
		$table->setFromArray(array($data));
		$table->setPartial(true);
		$table->setIncremental(true);
		$table->save();
	}
}