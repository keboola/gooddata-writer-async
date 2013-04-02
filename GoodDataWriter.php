<?php
/**
 * GoodDataWriter.php
 *
 * @author Jakub Matejka <jakub@keboola.com>
 * @created 2013-03-14
 */

namespace Keboola\GoodDataWriterBundle;


use Keboola\GoodDataWriterBundle\Writer\Configuration;
use Syrup\ComponentBundle\Component\Component;
use Symfony\Component\HttpFoundation\Request;
use Keboola\GoodDataWriterBundle\Writer\JobManager,
	Keboola\StorageApi\Table as StorageApiTable,
	Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Config\Reader,
	Keboola\Csv\CsvFile;
use Keboola\GoodDataWriterBundle\Exception\WrongParametersException,
	Keboola\GoodDataWriterBundle\Exception\WrongConfigurationException;

class GoodDataWriter extends Component
{
	protected $_roles = array(
		'admin' => 'adminRole',
		'editor' => 'editorRole',
		'readOnly' => 'readOnlyUserRole',
		'dashboardOnly' => 'dashboardOnlyRole'
	);

	protected $_name = 'gooddata';
	protected $_prefix = 'wr';


	/**
	 * @var Configuration
	 */
	public $configuration;

	/**
	 * @var StorageApiClient
	 */
	private $_sharedStorageApi;
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
		if (!file_exists($tmpDir)) {
			mkdir($tmpDir);
		}

		$this->configuration = new Configuration($params['writerId'], $this->_storageApi, $tmpDir);

		$this->_mainConfig = $this->_container->getParameter('gd_writer');

		$request = Request::createFromGlobals();
		$url = null;
		if ($request->headers->has('X-StorageApi-Url')) {
			$url = $request->headers->get('X-StorageApi-Url');
		}

		$this->_logUploader = $this->_container->get('syrup.monolog.s3_uploader');

		$queue = new Writer\Queue(new \Zend_Db_Adapter_Pdo_Mysql(array(
			'host' => $this->_mainConfig['db']['host'],
			'username' => $this->_mainConfig['db']['user'],
			'password' => $this->_mainConfig['db']['password'],
			'dbname' => $this->_mainConfig['db']['name']
		)));
		$sharedStorageApi = new StorageApiClient($this->_mainConfig['shared_token'], $url);
		$this->_jobManager = new JobManager(
			$queue,
			$this->configuration,
			$this->_storageApi,
			$sharedStorageApi,
			$this->_log,
			$this->_logUploader
		);
	}



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
			}
			if ($writerId && $foundWriterType) {
				$writers[] = array(
					'id' => $writerId,
					'bucket' => $bucket['id']
				);
				break;
			}
		}

		return array('writers' => $writers);
	}

	public function postWriters($params)
	{
		if (!isset($params['writerId'])) {
			throw new WrongParametersException('Missing parameter \'writerId\'');
		}

		$this->_init($params);

		if ($this->configuration->configurationBucket($params['writerId'])) {
			throw new WrongParametersException('Writer with id \'writerId\' already exists');
		}

		echo 1;

	}

	
	
	/***********************
	 * @section Projects
	 */


	/**
	 * List projects from configuration
	 * @param $params
	 * @return array
	 */
	public function getProjects($params)
	{
		$this->_init($params);
		$this->configuration->prepareProjects();

		$projects = array();
		$header = true;
		foreach ($this->configuration->projectsCsv as $p) {
			if (!$header) {
				$projects[] = array(
					'pid' => $p[0],
					'active' => $p[1]
				);
			} else {
				$header = false;
			}
		}
		return $projects;
	}


	/**
	 * Clone project
	 * @param $params
	 * @throws Exception\WrongConfigurationException
	 * @throws \Exception|Exception\RestApiException
	 * @return array
	 */
	public function postProjects($params)
	{
		$command = 'cloneProject';
		$createdTime = time();

		// Init parameters
		$this->_init($params);
		$accessToken = !empty($params['accessToken']) ? $params['accessToken'] : $this->_mainConfig['gd']['access_token'];
		$projectName = !empty($params['name']) ? $params['name']
			: sprintf($this->_mainConfig['gd']['project_name'], $this->configuration->tokenInfo['owner']['name'], $this->configuration->writerId);
		$this->configuration->prepareProjects();
		$mainProject = $this->configuration->projectsCsv->current();
		if (!$mainProject[1]) {
			throw new WrongConfigurationException('Main project is not active, check projects configuration table');
		}


		$restApi = new GoodData\RestApi($this->configuration->backendUrl, $this->_log);
		$jobId = $this->_jobManager->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'accessToken' => $accessToken,
				'projectName' => $projectName,
				'pidSource' => $mainProject[1]
			),
			'status' => 'processing'
		));

		try {
			$gdWriteStartTime = time();

			$restApi->login($this->_mainConfig['gd']['username'], $this->_mainConfig['gd']['gd.password']);

			// Get user uri if not set
			if (empty($this->configuration->bucketInfo['gd']['userUri'])) {
				$userUri = $restApi->userUri($this->configuration->bucketInfo['gd']['username'], $this->_mainConfig['gd']['domain']);
				$this->_storageApi->setBucketAttribute($this->configuration->bucketId, 'gd.userUri', $userUri);
				$this->configuration->bucketInfo['gd']['userUri'] = $userUri;
			}

			$projectPid = $restApi->createProject($projectName, $accessToken);

			$mainProject = $this->configuration->projectsCsv->current();
			$restApi->cloneProject($mainProject[0], $projectPid);

			$restApi->addUserToProject($this->configuration->bucketInfo['gd']['userUri'], $projectPid);
			$this->configuration->addProjectToConfiguration($projectPid);

			$logUrl = $this->_logUploader->uploadString('calls-' . $jobId, $restApi->callsLog());
			$this->_jobManager->finishJob($jobId, 'success', array(
				'gdWriteStartTime' => date('c', $gdWriteStartTime),
				'result' => array('pid' => $projectPid)
			), $logUrl);

			return array(
				'pid' => $projectPid
			);

		} catch (Exception\UnauthorizedException $e) {
			$this->_jobManager->finishJobWithError($jobId, $command, $restApi->callsLog(), $e);
			throw new WrongConfigurationException('Clone project failed');
		} catch (Exception\RestApiException $e) {
			$this->_jobManager->finishJobWithError($jobId, $command, $restApi->callsLog(), $e);
			throw $e;
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
		return $users;
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
		$this->configuration->prepareProjectUsers();
		$params['domain'] = $this->_mainConfig['gd']['domain'];


		$restApi = new GoodData\RestApi($this->configuration->backendUrl, $this->_log);
		$jobId = $this->_jobManager->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => $params,
			'status' => 'processing'
		));

		try {
			$gdWriteStartTime = time();

			$restApi->login($this->_mainConfig['gd']['username'], $this->_mainConfig['gd']['password']);
			$userUri = $restApi->userUri($params['email'], $params['domain']);

			$restApi->login($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);
			$restApi->addUserToProject($userUri, $params['pid'], $this->_roles[$params['role']]);

			$this->configuration->addProjectUserToConfiguration($params['pid'], $params['email'], $params['role']);

			$logUrl = $this->_logUploader->uploadString('calls-' . $jobId, $restApi->callsLog());
			$this->_jobManager->finishJob($jobId, 'success', array(
				'gdWriteStartTime' => date('c', $gdWriteStartTime)
			), $logUrl);

			return array();

		} catch (Exception\UnauthorizedException $e) {
			$this->_jobManager->finishJobWithError($jobId, $command, $restApi->callsLog(), $e);
			throw new WrongConfigurationException('Add user to project failed');
		} catch (Exception\RestApiException $e) {
			$this->_jobManager->finishJobWithError($jobId, $command, $restApi->callsLog(), $e);
			throw $e;
		}

	}

	
	
	/***********************
	 * @section Users
	 */

	/**
	 * List users from configuration
	 * @param $params
	 * @return array
	 */
	public function getUsers($params)
	{
		$this->_init($params);
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
		return $users;
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
		$params['domain'] = $this->_mainConfig['gd']['domain'];
		$this->_init($params);
		$this->configuration->prepareUsers();


		$restApi = new GoodData\RestApi($this->configuration->backendUrl, $this->_log);
		$jobId = $this->_jobManager->createJob(array(
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => $params,
			'status' => 'processing'
		));

		try {
			$gdWriteStartTime = time();

			$restApi->login($this->_mainConfig['gd']['username'], $this->_mainConfig['gd']['password']);

			$userUri = $restApi->createUserInDomain($params['domain'], $params['email'], $params['password'], $params['firstName'], $params['lastName']);
			$this->configuration->addUserToConfiguration($params['email'], $userUri);

			$logUrl = $this->_logUploader->uploadString('calls-' . $jobId, $restApi->callsLog());
			$this->_jobManager->finishJob($jobId, 'success', array(
				'gdWriteStartTime' => date('c', $gdWriteStartTime),
				'result' => array('uri' => $userUri)
			), $logUrl);

		} catch (Exception\UnauthorizedException $e) {
			$this->_jobManager->finishJobWithError($jobId, $command, $restApi->callsLog(), $e);
			throw new WrongConfigurationException('Create user failed');
		} catch (Exception\RestApiException $e) {
			$this->_jobManager->finishJobWithError($jobId, $command, $restApi->callsLog(), $e);
			throw $e;
		}

	}


}
