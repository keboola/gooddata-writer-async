<?php
/**
 * GoodDataWriter.php
 *
 * @author Jakub Matejka <jakub@keboola.com>
 * @created 2013-03-14
 */

namespace Keboola\GoodDataWriter;

use Keboola\GoodDataWriter\GoodData\RestApi;
use Monolog\Logger;
use Syrup\ComponentBundle\Component\Component;
use Symfony\Component\HttpFoundation\Request;
use Keboola\GoodDataWriter\Writer\Configuration,
	Keboola\StorageApi\Table as StorageApiTable,
	Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Config\Reader,
	Keboola\Csv\CsvFile;
use Keboola\GoodDataWriter\Exception\WrongParametersException,
	Keboola\GoodDataWriter\Exception\WrongConfigurationException;

class GoodDataWriter extends Component
{
	protected $_name = 'gooddata';
	protected $_prefix = 'wr';


	/**
	 * @var Configuration
	 */
	public $configuration;
	/**
	 * @var Writer\SharedConfig
	 */
	public $sharedConfig;

	/**
	 * @var array
	 */
	private $_mainConfig;
	/**
	 * @var \Syrup\ComponentBundle\Monolog\Uploader\SyrupS3Uploader
	 */
	private $_s3Uploader;
	/**
	 * @var Writer\Queue
	 */
	private $_queue;


	/**
	 * Init Writer
	 * @param $params
	 * @throws Exception\WrongParametersException
	 */
	private function _init($params)
	{
		// Init params
		if (!isset($params['writerId'])) {
			throw new WrongParametersException('Missing parameter \'writerId\'');
		}

		// Init main temp directory
		$tmpDir = $this->_container->get('kernel')->getRootDir() . '/tmp';
		$this->_mainConfig = $this->_container->getParameter('gd_writer');
		$this->_s3Uploader = $this->_container->get('syrup.monolog.s3_uploader');

		$this->configuration = new Configuration($params['writerId'], $this->_storageApi, $tmpDir);

		$this->_queue = new Writer\Queue(new \Zend_Db_Adapter_Pdo_Mysql(array(
			'host' => $this->_mainConfig['db']['host'],
			'username' => $this->_mainConfig['db']['user'],
			'password' => $this->_mainConfig['db']['password'],
			'dbname' => $this->_mainConfig['db']['name']
		)));

		$sharedStorageApi = new StorageApiClient($this->_mainConfig['shared_sapi']['token'], $this->_mainConfig['shared_sapi']['url']);
		$this->sharedConfig = new Writer\SharedConfig($sharedStorageApi);
	}


	/**
	 * List all configured writers
	 * @return array
	 */
	public function getWriters()
	{
		$writers = array();
		foreach ($this->_storageApi->listBuckets() as $bucket) {
			$writerId = false;
			$foundWriterType = false;
			if (isset($bucket['attributes']) && is_array($bucket['attributes'])) foreach($bucket['attributes'] as $attribute) {
				if ($attribute['name'] == 'writerId') {
					$writerId = $attribute['value'];
				}
				if ($attribute['name'] == 'writer') {
					$foundWriterType = $attribute['value'] == $this->_name;
				}
				if ($writerId && $foundWriterType) {
					break;
				}
			}
			if ($writerId && $foundWriterType) {
				$writers[] = array(
					'id' => $writerId,
					'bucket' => $bucket['id']
				);
			}
		}

		return array('writers' => $writers);
	}


	/**
	 * Create new writer with main GoodData project and user
	 * @param $params
	 * @return array
	 * @throws Exception\WrongParametersException
	 */
	public function postWriters($params)
	{
		$command = 'createWriter';
		$createdTime = time();

		if (!isset($params['writerId'])) {
			throw new WrongParametersException('Missing parameter \'writerId\'');
		}
		if (!preg_match('/^[a-zA-z0-9_]+$/', $params['writerId'])) {
			throw new WrongParametersException('Parameter writerId may contain only basic letters, numbers and underscores');
		}

		$this->_init($params);

		if ($this->configuration->configurationBucket($params['writerId'])) {
			throw new WrongParametersException('Writer with id \'writerId\' already exists');
		}

		$this->_storageApi->createBucket('wr-gooddata-' . $params['writerId'], 'sys', 'GoodData Writer Configuration');
		$this->_storageApi->setBucketAttribute('sys.c-wr-gooddata-' . $params['writerId'], 'writer', 'gooddata');
		$this->_storageApi->setBucketAttribute('sys.c-wr-gooddata-' . $params['writerId'], 'writerId', $params['writerId']);
		if (isset($params['backendUrl'])) {
			$this->_storageApi->setBucketAttribute('sys.c-wr-gooddata-' . $params['writerId'], 'gd.backendUrl', $params['backendUrl']);
		}
		$this->configuration->bucketId = 'sys.c-wr-gooddata-' . $params['writerId'];
		try {
			$this->configuration->prepareProjects();
		} catch (WrongConfigurationException $e) {
			// Ignore that table is empty
		}

		$mainConfig = empty($params['dev']) ? $this->_mainConfig['gd']['prod'] : $this->_mainConfig['gd']['dev'];
		$accessToken = !empty($params['accessToken']) ? $params['accessToken'] : $mainConfig['access_token'];
		$projectName = sprintf($mainConfig['project_name'], $this->configuration->tokenInfo['owner']['name'], $this->configuration->writerId);


		$jobInfo = $this->_createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'accessToken' => $accessToken,
				'projectName' => $projectName,
				'dev' => empty($params['dev']) ? 0 : 1
			)
		));
		$this->_queue->enqueueJob($jobInfo);

		return array('job' => $jobInfo['id']);
	}


	/**
	 * Delete writer with projects and users
	 * @param $params
	 * @return array
	 * @throws Exception\WrongParametersException
	 */
	public function postDeleteWriters($params)
	{
		$command = 'dropWriter';
		$createdTime = time();

		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		$jobInfo = $this->_createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'dev' => empty($params['dev']) ? 0 : 1
			)
		));
		$this->_queue->enqueueJob($jobInfo);

		return array('job' => $jobInfo['id']);
	}


	/***********************
	 * @section Projects
	 */


	/**
	 * List projects from configuration
	 * @param $params
	 * @throws Exception\WrongParametersException
	 * @return array
	 */
	public function getProjects($params)
	{
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}
		$this->configuration->prepareProjects();

		$projects = array();
		$header = true;
		foreach ($this->configuration->projectsCsv as $p) {
			if (!$header) {
				$projects[] = array(
					'pid' => $p[0],
					'active' => (int)$p[1]
				);
			} else {
				$header = false;
			}
		}
		return array('projects' => $projects);
	}


	/**
	 * Clone project
	 * @param $params
	 * @throws Exception\WrongParametersException
	 * @return array
	 */
	public function postProjects($params)
	{
		$command = 'cloneProject';
		$createdTime = time();

		// Init parameters
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}
		$mainConfig = empty($params['dev']) ? $this->_mainConfig['gd']['prod'] : $this->_mainConfig['gd']['dev'];
		$accessToken = !empty($params['accessToken']) ? $params['accessToken'] : $mainConfig['access_token'];
		$projectName = !empty($params['name']) ? $params['name']
			: sprintf($mainConfig['project_name'], $this->configuration->tokenInfo['owner']['name'], $this->configuration->writerId);
		$this->configuration->prepareProjects();
		$this->configuration->checkGoodDataSetup();


		$jobInfo = $this->_createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'accessToken' => $accessToken,
				'projectName' => $projectName,
				'includeData' => empty($params['includeData']) ? 0 : 1,
				'includeUsers' => empty($params['includeUsers']) ? 0 : 1,
				'pidSource' => $this->configuration->bucketInfo['gd']['pid'],
				'dev' => empty($params['dev']) ? 0 : 1
			)
		));
		$this->_queue->enqueueJob($jobInfo);


		if (empty($params['wait'])) {
			return array('job' => (int)$jobInfo['id']);
		} else {
			$jobId = $jobInfo['id'];
			$jobFinished = false;
			do {
				$jobInfo = $this->getJob(array('id' => $jobId, 'writerId' => $params['writerId']));
				if (isset($jobInfo['job']['status']) && ($jobInfo['job']['status'] == 'success' || $jobInfo['job']['status'] == 'error')) {
					$jobFinished = true;
				}
				if (!$jobFinished) sleep(30);
			} while(!$jobFinished);

			if ($jobInfo['job']['status'] == 'success' && isset($jobInfo['job']['result']['response']['pid'])) {
				return array('pid' => $jobInfo['job']['result']['response']['pid']);
			} else {
				return array('response' => $jobInfo['job']['result'], 'log' => $jobInfo['job']['log']);
			}
		}
	}



	/**
	 * List project users from configuration
	 * @param $params
	 * @throws Exception\WrongParametersException
	 * @return array
	 */
	public function getProjectUsers($params)
	{
		if (empty($params['pid'])) {
			throw new WrongParametersException("Parameter 'pid' is required");
		}

		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}
		$this->configuration->prepareProjectUsers();

		$users = array();
		$header = true;
		foreach ($this->configuration->projectUsersCsv as $u) {
			if (!$header && $u[1] == $params['pid']) {
				$users[] = array(
					'email' => $u[2],
					'role' => $u[3]
				);
			} else {
				$header = false;
			}
		}
		return array('users' => $users);
	}


	/**
	 * Add User to Project
	 * @param $params
	 * @return array
	 * @throws Exception\WrongConfigurationException
	 * @throws Exception\WrongParametersException
	 * @throws \Exception|GoodData\RestApiException
	 */
	public function postProjectUsers($params)
	{
		$command = 'addUserToProject';
		$createdTime = time();


		// Init parameters
		if (empty($params['email'])) {
			throw new WrongParametersException("Parameter 'email' is missing");
		}
		if (empty($params['pid'])) {
			throw new WrongParametersException("Parameter 'pid' is missing");
		}
		if (empty($params['role'])) {
			throw new WrongParametersException("Parameter 'role' is missing");
		}
		$allowedRoles = array_keys(RestApi::$userRoles);
		if (!in_array($params['role'], $allowedRoles)) {
			throw new WrongParametersException("Parameter 'role' is not valid; it has to be one of: " . implode(', ', $allowedRoles));
		}
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}
		$this->configuration->prepareProjectUsers();
		if (!$this->configuration->checkProject($params['pid'])) {
			throw new WrongParametersException(sprintf("Project '%s' is not configured for the writer", $params['pid']));
		}
		if (!$this->configuration->user($params['email'])) {
			throw new WrongParametersException(sprintf("User '%s' is not configured for the writer", $params['email']));
		}


		$jobInfo = $this->_createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => $params
		));
		$this->_queue->enqueueJob($jobInfo);


		if (empty($params['wait'])) {
			return array('job' => (int)$jobInfo['id']);
		} else {
			$jobId = $jobInfo['id'];
			$jobFinished = false;
			do {
				$jobInfo = $this->getJob(array('id' => $jobId, 'writerId' => $params['writerId']));
				if (isset($jobInfo['job']['status']) && ($jobInfo['job']['status'] == 'success' || $jobInfo['job']['status'] == 'error')) {
					$jobFinished = true;
				}
				if (!$jobFinished) sleep(30);
			} while(!$jobFinished);

			if ($jobInfo['job']['status'] == 'success') {
				return array();
			} else {
				return array('response' => $jobInfo['job']['result'], 'log' => $jobInfo['job']['log']);
			}
		}
	}

	/**
	 * Invite User to Project
	 * @param $params
	 * @return array
	 * @throws Exception\WrongConfigurationException
	 * @throws Exception\WrongParametersException
	 * @throws \Exception|GoodData\RestApiException
	 */
	public function postProjectInvitations($params)
	{
		$command = 'inviteUserToProject';
		$createdTime = time();


		// Init parameters
		if (empty($params['email'])) {
			throw new WrongParametersException("Parameter 'email' is missing");
		}
		if (empty($params['role'])) {
			throw new WrongParametersException("Parameter 'role' is missing");
		}
		$allowedRoles = array_keys(RestApi::$userRoles);
		if (!in_array($params['role'], $allowedRoles)) {
			throw new WrongParametersException("Parameter 'role' is not valid; it has to be one of: " . implode(', ', $allowedRoles));
		}
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}
		if (!empty($params['pid']) && !$this->configuration->checkProject($params['pid'])) {
			throw new WrongParametersException(sprintf("Project '%s' is not configured for the writer", $params['pid']));
		}
		$this->configuration->prepareProjectUsers();


		$jobInfo = $this->_createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => $params
		));
		$this->_queue->enqueueJob($jobInfo);


		if (empty($params['wait'])) {
			return array('job' => (int)$jobInfo['id']);
		} else {
			$jobId = $jobInfo['id'];
			$jobFinished = false;
			do {
				$jobInfo = $this->getJob(array('id' => $jobId, 'writerId' => $params['writerId']));
				if (isset($jobInfo['job']['status']) && ($jobInfo['job']['status'] == 'success' || $jobInfo['job']['status'] == 'error')) {
					$jobFinished = true;
				}
				if (!$jobFinished) sleep(30);
			} while(!$jobFinished);

			if ($jobInfo['job']['status'] == 'success') {
				return array();
			} else {
				return array('response' => $jobInfo['job']['result'], 'log' => $jobInfo['job']['log']);
			}
		}
	}

	
	
	/***********************
	 * @section Users
	 */

	/**
	 * List users from configuration
	 * @param $params
	 * @throws Exception\WrongParametersException
	 * @return array
	 */
	public function getUsers($params)
	{
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}
		$this->configuration->prepareUsers();

		$users = array();
		$header = true;
		foreach ($this->configuration->usersCsv as $u) {
			if (!$header) {
				$users[] = array(
					'email' => $u[0],
					'uri' => $u[1]
				);
			} else {
				$header = false;
			}
		}
		return array('users' => $users);
	}

	/**
	 * Create user
	 * @param $params
	 * @throws Exception\WrongConfigurationException
	 * @throws Exception\WrongParametersException
	 * @throws \Exception|GoodData\RestApiException
	 * @return array
	 */
	public function postUsers($params)
	{
		$command = 'createUser';
		$createdTime = time();


		// Init parameters
		if (empty($params['firstName'])) {
			throw new WrongParametersException("Parameter 'firstName' is missing");
		}
		if (empty($params['lastName'])) {
			throw new WrongParametersException("Parameter 'lastName' is missing");
		}
		if (empty($params['email'])) {
			throw new WrongParametersException("Parameter 'email' is missing");
		}
		if (empty($params['password'])) {
			throw new WrongParametersException("Parameter 'password' is missing");
		}
		if (strlen($params['password']) < 7) {
			throw new WrongParametersException("Parameter 'password' must have at least seven characters");
		}
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}
		$this->configuration->prepareUsers();


		$jobInfo = $this->_createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => $params
		));

		$this->_queue->enqueueJob($jobInfo);


		if (empty($params['wait'])) {
			return array('job' => (int)$jobInfo['id']);
		} else {
			$jobId = $jobInfo['id'];
			$jobFinished = false;
			do {
				$jobInfo = $this->getJob(array('id' => $jobId, 'writerId' => $params['writerId']));
				if (isset($jobInfo['job']['status']) && ($jobInfo['job']['status'] == 'success' || $jobInfo['job']['status'] == 'error')) {
					$jobFinished = true;
				}
				if (!$jobFinished) sleep(30);
			} while(!$jobFinished);

			if ($jobInfo['job']['status'] == 'success' && isset($jobInfo['job']['result']['response']['uri'])) {
				return array('uri' => $jobInfo['job']['result']['response']['uri']);
			} else {
				return array('response' => $jobInfo['job']['result'], 'log' => $jobInfo['job']['log']);
			}
		}
	}



	/***********************
	 * @section Data and project structure
	 */

	public function getXml($params)
	{
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}
		if (empty($params['tableId'])) {
			throw new WrongParametersException("Parameter 'tableId' is missing");
		}

		echo $this->configuration->getXml($params['tableId']);
		exit(); //@TODO
	}

	public function postUploadTable($params)
	{die('Not yet implemented');
		$createdTime = time();

		// Init parameters
		if (empty($params['tableId'])) {
			throw new WrongParametersException("Parameter 'tableId' is missing");
		}
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		$this->configuration->checkGoodDataSetup();

		$xml = $this->configuration->getXml($params['tableId']);
		$xmlUrl = $this->_s3Uploader->uploadString($params['tableId'] . '.xml', $xml, 'text/xml');

		$runId = $this->_storageApi->getRunId();

		$pid = null; //@TODO PID

		// Create used date dimensions
		$dateDimensions = null;
		$xmlObject = simplexml_load_string($xml);
		foreach ($xmlObject->columns->column as $column) if ((string)$column->ldmType == 'DATE') {
			if (!$dateDimensions) {
				$dateDimensions = $this->configuration->getDateDimensions();
			}

			$dimension = (string)$column->schemaReference;
			if (!isset($dateDimensions[$dimension])) {
				throw new WrongConfigurationException("Date dimension '$dimension' does not exist");
			}

			if (empty($dateDimensions[$dimension]['lastExportDate'])) {
				$jobInfo = $this->_createJob(array(
					'runId' => $runId,
					'command' => 'createDate',
					'dataset' => $dimension,
					'pid' => $pid,
					'createdTime' => date('c', $createdTime),
					'parameters' => array('includeTime' => !empty($dateDimensions[$dimension]['includeTime']))
				));

				$this->configuration->setDateDimensionAttribute($dimension, 'lastExportDate', date('c'));
				$this->_queue->enqueueJob($jobInfo);
			}
		}

		$tableDefinition = $this->configuration->getTableDefinition($params['tableId']);
		if (empty($tableDefinition['lastExportDate'])) {
			//@TODO Create dataset
		} else if (empty($tableDefinition['lastChangeDate']) || strtotime($tableDefinition['lastChangeDate']) > strtotime($tableDefinition['lastExportDate'])) {
			//@TODO Update dataset
		}

		//@TODO Load data


		if (empty($params['wait'])) {
			return array('batch' => (int)$runId);
		} else {
			$batchFinished = false;
			do {
				$jobInfo = $this->getBatch(array('id' => $runId, 'writerId' => $params['writerId']));
				if (isset($jobInfo['job']['status']) && ($jobInfo['job']['status'] == 'success' || $jobInfo['job']['status'] == 'error')) {
					$jobFinished = true;
				}
				if (!$batchFinished) sleep(30);
			} while(!$batchFinished);

			if ($jobInfo['job']['status'] == 'success' && isset($jobInfo['job']['result']['response']['uri'])) {
				return array('uri' => $jobInfo['job']['result']['response']['uri']);
			} else {
				return array('response' => $jobInfo['job']['result'], 'log' => $jobInfo['job']['log']);
			}
		}
	}





	/***********************
	 * @section Jobs
	 */

	/**
	 * Get Job
	 * @param $params
	 * @throws Exception\WrongParametersException
	 * @return array
	 */
	public function getJob($params)
	{
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}
		if (empty($params['id'])) {
			throw new WrongParametersException("Parameter 'id' is missing");
		}

		$job = $this->sharedConfig->fetchJob($params['id']);
		if ($job['projectId'] != $this->configuration->projectId || $job['writerId'] != $this->configuration->writerId) {
			throw new WrongParametersException(sprintf("Job '%d' does not belong to writer '%s'", $params['id'], $this->configuration->writerId));
		}
		$job = $this->sharedConfig->jobToApiResponse($job);
		return array('job' => $job);
	}

	/**
	 * Get Batch
	 * @param $params
	 * @throws Exception\WrongParametersException
	 * @return array
	 */
	public function getBatch($params)
	{
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}
		if (empty($params['id'])) {
			throw new WrongParametersException("Parameter 'id' is missing");
		}

		$data = array(
			'runId' => (int)$params['id'],
			'createdTime' => date('c'),
			'startTime' => date('c'),
			'endTime' => null,
			'status' => null,
			'jobs' => array()
		);
		$waitingJobs = 0;
		$processingJobs = 0;
		$errorJobs = 0;
		$successJobs = 0;
		foreach ($this->sharedConfig->fetchBatch($params['id']) as $job) {
			if ($job['projectId'] != $this->configuration->projectId || $job['writerId'] != $this->configuration->writerId) {
				throw new WrongParametersException(sprintf("Job '%d' does not belong to writer '%s'", $params['id'], $this->configuration->writerId));
			}

			if ($job['createdTime'] < $data['createdTime']) $data['createdTime'] = $job['createdTime'];
			if ($job['startTime'] < $data['startTime']) $data['startTime'] = $job['startTime'];
			if ($job['endTime'] > $data['endTime']) $data['endTime'] = $job['endTime'];
			$data['jobs'][] = (int)$job['id'];
			if ($job['status'] == 'waiting') $waitingJobs++;
				elseif ($job['status'] == 'processing') $processingJobs++;
				elseif ($job['status'] == 'error') $errorJobs++;
				else $successJobs++;
		}

		if ($processingJobs > 0) $data['status'] = 'processing';
			elseif ($waitingJobs > 0) $data['status'] = 'waiting';
			elseif ($errorJobs > 0) $data['status'] = 'error';
			else $data['status'] = 'success';

		return array('batch' => $data);
	}




	private function _createJob($params)
	{
		if (!isset($params['runId'])) {
			$params['runId'] = $this->_storageApi->getRunId();
		}
		$jobId = $this->_storageApi->generateId();
		$jobInfo = array(
			'id' => $jobId,
			'projectId' => $this->configuration->projectId,
			'writerId' => $this->configuration->writerId,
			'sapiUrl' => $this->_storageApi->getApiUrl(),
			'token' => $this->_storageApi->token,
			'tokenId' => $this->configuration->tokenInfo['id'],
			'tokenDesc' => $this->configuration->tokenInfo['description'],
			'tokenOwnerName' => $this->configuration->tokenInfo['owner']['name'],
			'initializedBy' => null,
			'createdTime' => null,
			'startTime' => null,
			'endTime' => null,
			'backendUrl' => $this->configuration->backendUrl,
			'pid' => null,
			'command' => null,
			'dataset' => null,
			'xmlFile' => null,
			'csvFile' => null,
			'parameters' => null,
			'result' => null,
			'gdWriteStartTime' => null,
			'gdWriteBytes' => null,
			'status' => 'waiting',
			'log' => null
		);
		$jobInfo = array_merge($jobInfo, $params);
		$this->sharedConfig->saveJob($jobId, $jobInfo);

		$message = "Writer job $jobId created manually";
		$results = array('jobId' => $jobId);
		$this->sharedConfig->logEvent($this->configuration->writerId, $params['runId'], $message, $params, $results);

		$this->_log->log(Logger::INFO, $message, array(
			'token' => $this->_storageApi->getLogData(),
			'configurationId' => $this->configuration->writerId,
			'runId' => $params['runId'],
			'params' => $params,
			'results' => $results
		));

		return $jobInfo;
	}

}
