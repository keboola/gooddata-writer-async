<?php
/**
 * GoodDataWriter.php
 *
 * @author Jakub Matejka <jakub@keboola.com>
 * @created 2013-03-14
 */

namespace Keboola\GoodDataWriterBundle;

use Syrup\ComponentBundle\Component\Component;
use Symfony\Component\HttpFoundation\Request;
use Keboola\GoodDataWriterBundle\Writer\Configuration,
	Keboola\GoodDataWriterBundle\Writer\JobManager,
	Keboola\StorageApi\Table as StorageApiTable,
	Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Config\Reader,
	Keboola\Csv\CsvFile;
use Keboola\GoodDataWriterBundle\Exception\WrongParametersException,
	Keboola\GoodDataWriterBundle\Exception\WrongConfigurationException;

class GoodDataWriter extends Component
{
	protected $_name = 'gooddata';
	protected $_prefix = 'wr';

	protected $_roles = array(
		'admin' => 'adminRole',
		'editor' => 'editorRole',
		'readOnly' => 'readOnlyUserRole',
		'dashboardOnly' => 'dashboardOnlyRole'
	);


	/**
	 * @var Configuration
	 */
	public $configuration;

	/**
	 * @var JobManager
	 */
	private $_jobManager;
	/**
	 * @var array
	 */
	private $_mainConfig;
	/**
	 * @var \Syrup\ComponentBundle\Monolog\Uploader\SyrupS3Uploader
	 */
	private $_logUploader;
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
		$this->_logUploader = $this->_container->get('syrup.monolog.s3_uploader');
		$sharedStorageApi = new StorageApiClient($this->_mainConfig['shared_sapi']['token'], $this->_mainConfig['shared_sapi']['url']);


		$this->configuration = new Configuration($params['writerId'], $this->_storageApi, $tmpDir);

		$this->_queue = new Writer\Queue(new \Zend_Db_Adapter_Pdo_Mysql(array(
			'host' => $this->_mainConfig['db']['host'],
			'username' => $this->_mainConfig['db']['user'],
			'password' => $this->_mainConfig['db']['password'],
			'dbname' => $this->_mainConfig['db']['name']
		)));

		$this->_jobManager = new JobManager(
			$this->configuration,
			$this->_storageApi,
			$sharedStorageApi,
			$this->_log
		);
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


		$jobInfo = $this->_jobManager->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'accessToken' => $accessToken,
				'projectName' => $projectName,
				'dev' => empty($params['dev'])
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

		$jobInfo = $this->_jobManager->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'dev' => empty($params['dev'])
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


		$jobInfo = $this->_jobManager->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'accessToken' => $accessToken,
				'projectName' => $projectName,
				'pidSource' => $this->configuration->bucketInfo['gd']['pid'],
				'dev' => empty($params['dev'])
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
	 * @throws \Exception|Exception\RestApiException
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
		if (!in_array($params['role'], array_keys($this->_roles))) {
			throw new WrongParametersException("Parameter 'role' is not valid; it has to be one of: " . implode(', ', array_keys($this->_roles)));
		}
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}
		$this->configuration->prepareProjectUsers();
		if (!$this->configuration->checkProject($params['pid'])) {
			throw new WrongParametersException(sprintf("Project '%s' is not configured for the writer", $params['pid']));
		}
		if (!$this->configuration->checkUser($params['email'])) {
			throw new WrongParametersException(sprintf("User '%s' is not configured for the writer", $params['email']));
		}


		$jobInfo = $this->_jobManager->createJob(array(
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
	 * @throws \Exception|Exception\RestApiException
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
		if (!in_array($params['role'], array_keys($this->_roles))) {
			throw new WrongParametersException("Parameter 'role' is not valid; it has to be one of: " . implode(', ', array_keys($this->_roles)));
		}
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}
		if (!empty($params['pid']) && !$this->configuration->checkProject($params['pid'])) {
			throw new WrongParametersException(sprintf("Project '%s' is not configured for the writer", $params['pid']));
		}
		$this->configuration->prepareProjectUsers();


		$jobInfo = $this->_jobManager->createJob(array(
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
	 * @throws \Exception|Exception\RestApiException
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


		$jobInfo = $this->_jobManager->createJob(array(
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

		$job = $this->_jobManager->fetchJob($params['id']);
		if ($job['projectId'] != $this->configuration->projectId || $job['writerId'] != $this->configuration->writerId) {
			throw new WrongParametersException(sprintf("Job '%d' does not belong to writer '%s'", $params['id'], $this->configuration->writerId));
		}
		$job = $this->_jobManager->jobToApiResponse($job);
		return array('job' => $job);
	}


}
