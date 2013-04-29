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
use Keboola\GoodDataWriter\Exception\JobProcessException,
	Keboola\GoodDataWriter\Exception\WrongParametersException,
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
		$this->_mainConfig = $this->_container->getParameter('gooddata_writer');
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
	 * @throws Exception\JobProcessException
	 * @throws Exception\WrongParametersException
	 * @return array
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

			if ($jobInfo['job']['status'] == 'success' && isset($jobInfo['job']['result']['pid'])) {
				return array('pid' => $jobInfo['job']['result']['pid']);
			} else {
				$e = new JobProcessException('Create Writer job failed');
				$e->setData(array('result' => $jobInfo['job']['result'], 'log' => $jobInfo['job']['log']));
				throw $e;
			}
		}
	}


	/**
	 * Delete writer with projects and users
	 * @param $params
	 * @throws Exception\JobProcessException
	 * @throws Exception\WrongParametersException
	 * @return array
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
				$e = new JobProcessException('Delete Writer job failed');
				$e->setData(array('result' => $jobInfo['job']['result'], 'log' => $jobInfo['job']['log']));
				throw $e;
			}
		}
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

		return array('projects' => $this->configuration->getProjects());
	}


	/**
	 * Clone project
	 * @param $params
	 * @throws Exception\JobProcessException
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
		$this->configuration->checkGoodDataSetup();
		$this->configuration->checkProjectsTable();


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

			if ($jobInfo['job']['status'] == 'success' && isset($jobInfo['job']['result']['pid'])) {
				return array('pid' => $jobInfo['job']['result']['pid']);
			} else {
				$e = new JobProcessException('Create Project job failed');
				$e->setData(array('result' => $jobInfo['job']['result'], 'log' => $jobInfo['job']['log']));
				throw $e;
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

		$users = array();
		foreach ($this->configuration->getProjectUsers() as $u) {
			if ($u['pid'] == $params['pid']) {
				$users[] = array(
					'email' => $u['email'],
					'role' => $u['role']
				);
			}
		}
		return array('users' => $users);
	}


	/**
	 * Add User to Project
	 * @param $params
	 * @throws Exception\JobProcessException
	 * @throws Exception\WrongParametersException
	 * @return array
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
		if (!$this->configuration->getProject($params['pid'])) {
			throw new WrongParametersException(sprintf("Project '%s' is not configured for the writer", $params['pid']));
		}
		if (!$this->configuration->getUser($params['email'])) {
			throw new WrongParametersException(sprintf("User '%s' is not configured for the writer", $params['email']));
		}
		$this->configuration->checkProjectUsersTable();


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
				$e = new JobProcessException('Create Project User job failed');
				$e->setData(array('result' => $jobInfo['job']['result'], 'log' => $jobInfo['job']['log']));
				throw $e;
			}
		}
	}

	/**
	 * Invite User to Project
	 * @param $params
	 * @throws Exception\JobProcessException
	 * @throws Exception\WrongParametersException
	 * @return array
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
		if (!empty($params['pid']) && !$this->configuration->getProject($params['pid'])) {
			throw new WrongParametersException(sprintf("Project '%s' is not configured for the writer", $params['pid']));
		}
		$this->configuration->checkProjectUsersTable();


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
				$e = new JobProcessException('Invite User job failed');
				$e->setData(array('result' => $jobInfo['job']['result'], 'log' => $jobInfo['job']['log']));
				throw $e;
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

		return array('users' => $this->configuration->getUsers());
	}

	/**
	 * Create user
	 * @param $params
	 * @throws Exception\JobProcessException
	 * @throws Exception\WrongParametersException
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
		$this->configuration->checkUsersTable();


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

			if ($jobInfo['job']['status'] == 'success' && isset($jobInfo['job']['result']['uid'])) {
				return array('uid' => $jobInfo['job']['result']['uid']);
			} else {
				$e = new JobProcessException('Create User job failed');
				$e->setData(array('result' => $jobInfo['job']['result'], 'log' => $jobInfo['job']['log']));
				throw $e;
			}
		}
	}



	/***********************
	 * @section Data and project structure
	 */

	/**
	 * @param $params
	 * @throws Exception\WrongParametersException
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
		exit();
	}

	/**
	 * @param $params
	 * @return array
	 * @throws Exception\JobProcessException
	 * @throws Exception\WrongParametersException
	 */
	public function postUploadTable($params)
	{
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
		$this->configuration->getDateDimensions();

		$xml = $this->configuration->getXml($params['tableId']);
		$xmlUrl = $this->_s3Uploader->uploadString($params['tableId'] . '.xml', $xml, 'text/xml', false);

		$tableDefinition = $this->configuration->getTableDefinition($params['tableId']);
		$jobData = array(
			'command' => 'uploadTable',
			'dataset' => !empty($tableDefinition['gdName']) ? $tableDefinition['gdName'] : $tableDefinition['tableId'],
			'createdTime' => date('c', $createdTime),
			'xmlFile' => $xmlUrl,
			'parameters' => array(
				'tableId' => $params['tableId']
			)
		);
		if (isset($params['incremental'])) {
			$jobData['parameters']['incremental'] = $params['incremental'];
		}
		if (isset($params['sanitize'])) {
			$jobData['parameters']['sanitize'] = $params['sanitize'];
		}
		$jobInfo = $this->_createJob($jobData);
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
				$e = new JobProcessException('Upload Table job failed');
				$e->setData(array('result' => $jobInfo['job']['result'], 'log' => $jobInfo['job']['log']));
				throw $e;
			}
		}
	}

	/**
	 * @param $params
	 * @return array
	 * @throws Exception\JobProcessException
	 * @throws Exception\WrongParametersException
	 * @throws Exception\WrongConfigurationException
	 */
	public function postUploadProject($params)
	{
		$createdTime = time();

		// Init parameters
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		$this->configuration->checkGoodDataSetup();
		$this->configuration->getDateDimensions();
		$runId = $this->_storageApi->getRunId();


		// Get tables XML and check them for errors
		$tables = array();
		foreach ($this->configuration->definedTables as $tableInfo) if (!empty($tableInfo['export'])) {

			$xml = $this->configuration->getXml($tableInfo['tableId']);
			$xmlUrl = $this->_s3Uploader->uploadString($tableInfo['tableId'] . '.xml', $xml, 'text/xml', false);
			$definition = $this->configuration->getTableDefinition($tableInfo['tableId']);

			$tables[$tableInfo['tableId']] = array(
				'dataset'		=> !empty($tableInfo['gdName']) ? $tableInfo['gdName'] : $tableInfo['tableId'],
				'tableId'		=> $tableInfo['tableId'],
				'xml'			=> $xmlUrl,
				'definition'	=> $definition['columns']
			);
		}


		// Sort tables for GD export according to their references
		$unsortedTables = array();
		$sortedTables = array();
		$references = array();
		$allTableIds = array_keys($tables);
		foreach ($tables as $tableId => $tableConfig) {
			$unsortedTables[$tableId] = $tableConfig;
			foreach ($tableConfig['definition'] as $c) if (!empty($c['schemaReference'])) {
				if (in_array($c['schemaReference'], $allTableIds)) {
					$references[$tableId][] = $c['schemaReference'];
				} else {
					throw new WrongConfigurationException("Schema reference '{$c['schemaReference']}' for table '{$tableId}'does not exist");
				}
			}
		}

		$ttl = 20;
		while (count($unsortedTables)) {
			foreach ($unsortedTables as $tableId => $tableConfig) {
				$areSortedReferences = TRUE;
				if (isset($references[$tableId])) foreach($references[$tableId] as $r) {
					if (!array_key_exists($r, $sortedTables)) {
						$areSortedReferences = FALSE;
					}
				}
				if ($areSortedReferences) {
					$sortedTables[$tableId] = $tableConfig;
					unset($unsortedTables[$tableId]);
				}
			}
			$ttl--;

			if ($ttl <= 0) {
				throw new WrongConfigurationException('Check of references failed with timeout. You probably have a recursion in tables references');
			}
		}

		foreach ($sortedTables as $table) {
			$jobData = array(
				'runId' => $runId,
				'command' => 'uploadTable',
				'dataset' => $table['dataset'],
				'createdTime' => date('c', $createdTime),
				'xmlFile' => $table['xml'],
				'parameters' => array(
					'tableId' => $table['tableId']
				)
			);
			if (isset($params['incremental'])) {
				$jobData['parameters']['incremental'] = $params['incremental'];
			}
			if (isset($params['sanitize'])) {
				$jobData['parameters']['sanitize'] = $params['sanitize'];
			}
			$jobInfo = $this->_createJob($jobData);
			$this->_queue->enqueueJob($jobInfo);
		}

		// Execute reports
		$jobData = array(
			'runId' => $runId,
			'command' => 'executeReports',
			'createdTime' => date('c', $createdTime)
		);
		$jobInfo = $this->_createJob($jobData);
		$this->_queue->enqueueJob($jobInfo);



		if (empty($params['wait'])) {
			return array('batch' => (int)$runId);
		} else {
			$jobsFinished = false;
			do {
				$jobsInfo = $this->getBatch(array('id' => $runId, 'writerId' => $params['writerId']));
				if (isset($jobsInfo['batch']['status']) && ($jobsInfo['batch']['status'] == 'success' || $jobsInfo['batch']['status'] == 'error')) {
					$jobsFinished = true;
				}
				if (!$jobsFinished) sleep(30);
			} while(!$jobsFinished);

			if ($jobsInfo['batch']['status'] == 'success') {
				return array();
			} else {
				$e = new JobProcessException('Upload Project job failed');
				$e->setData(array('result' => $jobsInfo['batch']['result'], 'log' => $jobsInfo['batch']['log']));
				throw $e;
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
			elseif ($job['status'] == 'error') {
				$errorJobs++;
				$data['result'] = $job['result'];
			}
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
		$jobId = $this->_storageApi->generateId();
		if (!isset($params['runId'])) {
			$params['runId'] = $jobId;
		}
		$jobInfo = array(
			'id' => $jobId,
			'projectId' => $this->configuration->projectId,
			'writerId' => $this->configuration->writerId,
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
