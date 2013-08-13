<?php
/**
 * GoodDataWriter.php
 *
 * @author Jakub Matejka <jakub@keboola.com>
 * @created 2013-03-14
 */

namespace Keboola\GoodDataWriter;

use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\GoodData\SSO;
use Monolog\Logger;
use Symfony\Component\HttpFoundation\ResponseHeaderBag;
use Symfony\Component\HttpFoundation\StreamedResponse;
use Syrup\ComponentBundle\Component\Component;
use Symfony\Component\HttpFoundation\Request,
	Symfony\Component\HttpFoundation\Response;
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
	 * @var Service\S3Client
	 */
	private $_s3Client;
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
		$this->_mainConfig = $this->_container->getParameter('gooddata_writer');
		$tmpDir = $this->_mainConfig['tmp_path'];

		$this->configuration = new Configuration($params['writerId'], $this->_storageApi, $tmpDir);

		$this->_s3Client = new Service\S3Client($this->_mainConfig['s3']['access_key'], $this->_mainConfig['s3']['secret_key'],
			$this->_mainConfig['s3']['bucket'], $this->configuration->projectId . '.' . $this->configuration->writerId);

		$this->_queue = new Writer\Queue(new \Zend_Db_Adapter_Pdo_Mysql(array(
			'host' => $this->_mainConfig['db']['host'],
			'username' => $this->_mainConfig['db']['user'],
			'password' => $this->_mainConfig['db']['password'],
			'dbname' => $this->_mainConfig['db']['name']
		)));

		$sharedStorageApi = new StorageApiClient(
			$this->_mainConfig['shared_sapi']['token'],
			$this->_mainConfig['shared_sapi']['url'],
			$this->_mainConfig['user_agent']
		);
		$this->sharedConfig = new Writer\SharedConfig($sharedStorageApi);
	}


	/**
	 * List all configured writers
	 *
	 * @param $params
	 * @throws Exception\WrongParametersException
	 * @return array
	 */
	public function getWriters($params)
	{
		if (isset($params['writerId'])) {
			$this->_init($params);
			if (!$this->configuration->bucketId) {
				throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
			}

			return array('writer' => $this->configuration->bucketInfo);
		} else {
			return array('writers' => Configuration::getWriters($this->_storageApi));
		}
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

		$this->configuration->createWriter($params['writerId'], isset($params['backendUrl']) ? $params['backendUrl'] : null);

		$mainConfig = empty($params['dev']) ? $this->_mainConfig['gd']['prod'] : $this->_mainConfig['gd']['dev'];
		$accessToken = !empty($params['accessToken']) ? $params['accessToken'] : $mainConfig['access_token'];
		$projectName = sprintf($mainConfig['project_name'], $this->configuration->tokenInfo['owner']['name'], $this->configuration->writerId);


		$batchId = $this->_storageApi->generateId();
		$jobInfo = $this->_createJob(array(
			'batchId' => $batchId,
			'command' => $command,
			'createdTime' => date('c', $createdTime),
			'parameters' => array(
				'accessToken' => $accessToken,
				'projectName' => $projectName,
				'dev' => empty($params['dev']) ? 0 : 1
			)
		));
		$this->_queue->enqueueJob($jobInfo);

		if(empty($params['users'])) {
			if (empty($params['wait'])) {
				return array('job' => (int)$jobInfo['id']);
			} else {
				$result = $this->_waitForJob($jobInfo['id'], $params['writerId']);
				if (isset($result['job']['result']['pid'])) {
					return array('pid' => $result['job']['result']['pid']);
				} else {
					$e = new JobProcessException('Job failed');
					$e->setData(array('result' => $result['job']['result'], 'log' => $result['job']['log']));
					throw $e;
				}
			}
		} else {

			$users = explode(',', $params['users']);
			foreach ($users as $user) {
				$job = $this->_createJob(array(
					'batchId' => $batchId,
					'command' => 'inviteUserToProject',
					'createdTime' => date('c', $createdTime),
					'parameters' => array(
						'email' => $user,
						'role' => 'admin'
					)
				));
				$this->_queue->enqueueJob($job);
			}

			return array('batch' => (int)$batchId);
		}


	}


	/**
	 * Delete writer with projects and users
	 * @param $params
	 * @throws Exception\JobProcessException
	 * @throws Exception\WrongParametersException
	 * @return array
	 */
	public function deleteWriters($params)
	{
		$command = 'dropWriter';
		$createdTime = time();

		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		$this->configuration->setBucketAttribute('toDelete', '1');

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
			$this->_waitForJob($jobInfo['id'], $params['writerId']);
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
			$result = $this->_waitForJob($jobInfo['id'], $params['writerId']);
			if (isset($result['job']['result']['pid'])) {
				return array('pid' => $result['job']['result']['pid']);
			} else {
				$e = new JobProcessException('Job failed');
				$e->setData(array('result' => $result['job']['result'], 'log' => $result['job']['log']));
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

		return array('users' => $this->configuration->getProjectUsers($params['pid']));
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
		$this->configuration->checkGoodDataSetup();
		$this->configuration->checkProjectsTable();
		$this->configuration->checkUsersTable();
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
			$this->_waitForJob($jobInfo['id'], $params['writerId']);
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
			$this->_waitForJob($jobInfo['id'], $params['writerId']);
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
		$this->configuration->checkGoodDataSetup();
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
			$result = $this->_waitForJob($jobInfo['id'], $params['writerId']);
			if (isset($result['job']['result']['uid'])) {
				return array('uid' => $result['job']['result']['uid']);
			} else {
				$e = new JobProcessException('Job failed');
				$e->setData(array('result' => $result['job']['result'], 'log' => $result['job']['log']));
				throw $e;
			}
		}
	}

	public function getSso($params)
	{
		// Init parameters
		if (empty($params['email'])) {
			throw new WrongParametersException("Parameter 'email' is missing");
		}
		if (empty($params['pid'])) {
			throw new WrongParametersException("Parameter 'pid' is missing");
		}
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}
		if (!empty($params['pid']) && !$this->configuration->getProject($params['pid'])) {
			throw new WrongParametersException(sprintf("Project '%s' is not configured for the writer", $params['pid']));
		}

		if (!empty($params['createUser']) && $params['createUser'] = 1) {
			$params['wait'] = 1;
			$this->postUsers($params);
			$this->postProjectUsers($params);
		}

		$user = $this->configuration->getUser($params['email']);
		if (!$user) {
			throw new WrongParametersException("User " . $user . " doesn't exist in writer");
		}

		$mainConfig = empty($params['dev']) ? $this->_mainConfig['gd']['prod'] : $this->_mainConfig['gd']['dev'];
		$sso = new SSO($this->configuration, $mainConfig);

		$gdProjectUrl = '/#s=/gdc/projects/' . $params['pid'];
		$ssoLink = $sso->url($gdProjectUrl, $params['email']);

		return array('ssoLink' => $ssoLink);
	}

	/***********************
	 * @section Filters
	 */


	/**
	 * Returns list of filters configured in writer
	 * If 'userEmail' parameter is specified, only returns filters for specified user
	 *
	 * @param $params
	 * @return array
	 * @throws Exception\WrongParametersException
	 */
	public function getFilters($params)
	{
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		if (isset($params['userEmail'])) {
			if (isset($params['pid'])) {
				$filters = $this->configuration->getFiltersForUser($params['userEmail'], $params['pid']);
			} else {
				$filters = $this->configuration->getFiltersForUser($params['userEmail']);
			}
		} else {
			$filters = $this->configuration->getFilters();
		}

		return array('filters' => $filters);
	}

	/**
	 * Create new user filter
	 *
	 * @param $params
	 * @return array
	 * @throws Exception\JobProcessException
	 * @throws Exception\WrongParametersException
	 */
	public function postFilters($params)
	{
		$command = 'createFilter';
		$createdTime = time();

		// Init parameters
		if (empty($params['name'])) {
			throw new WrongParametersException("Parameter 'name' is missing");
		}
		if (empty($params['attribute'])) {
			throw new WrongParametersException("Parameter 'attribute' is missing");
		}
		if (empty($params['element'])) {
			throw new WrongParametersException("Parameter 'element' is missing");
		}
		if (empty($params['pid'])) {
			throw new WrongParametersException("Parameter 'pid' is missing");
		}
		if (!isset($params['operator'])) {
			$params['operator'] = '=';
		}

		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
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
			$result = $this->_waitForJob($jobInfo['id'], $params['writerId']);

			if (isset($result['job']['result']['uri'])) {
				return array('uri' => $result['job']['result']['uri']);
			} else {
				$e = new JobProcessException('Job failed');
				$e->setData(array('result' => $result['job']['result'], 'log' => $result['job']['log']));
				throw $e;
			}
		}
	}

	public function deleteFilters($params)
	{
		$command = 'deleteFilter';
		$createdTime = time();

		// Init parameters
		if (empty($params['uri'])) {
			throw new WrongParametersException("Parameter 'uri' is missing");
		}

		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
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
			$result = $this->_waitForJob($jobInfo['id'], $params['writerId']);
			if (isset($result['job']['result']['uri'])) {
				return array('uri' => $result['job']['result']['uri']);
			} else {
				$e = new JobProcessException('Job failed');
				$e->setData(array('result' => $result['job']['result'], 'log' => $result['job']['log']));
				throw $e;
			}
		}
	}

	/**
	 * Assign filter to user
	 *
	 * @param $params
	 * @return array
	 * @throws Exception\JobProcessException
	 * @throws Exception\WrongParametersException
	 */
	public function postFiltersUser($params)
	{
		$command = 'assignFiltersToUser';
		$createdTime = time();

		if (empty($params['filters'])) {
			throw new WrongParametersException("Parameter 'filters' is missing");
		}
		if (empty($params['userEmail'])) {
			throw new WrongParametersException("Parameter 'userEmail' is missing");
		}

		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
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
			$result = $this->_waitForJob($jobInfo['id'], $params['writerId']);
			if (isset($result['job']['result']['uri'])) {
				return array('uri' => $result['job']['result']['uri']);
			} else {
				$e = new JobProcessException('Job failed');
				$e->setData(array('result' => $result['job']['result'], 'log' => $result['job']['log']));
				throw $e;
			}
		}
	}

	/**
	 * Synchronize filters from writer's configuration to GoodData project
	 *
	 * @param $params
	 * @return array
	 * @throws Exception\JobProcessException
	 * @throws Exception\WrongParametersException
	 */
	public function postSyncFilters($params)
	{
		$command = 'syncFilters';
		$createdTime = time();

		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
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
			$result = $this->_waitForJob($jobInfo['id'], $params['writerId']);
			if (isset($result['job']['result']['uri'])) {
				return array('uri' => $result['job']['result']['uri']);
			} else {
				$e = new JobProcessException('Job failed');
				$e->setData(array('result' => $result['job']['result'], 'log' => $result['job']['log']));
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

		$response = new Response($this->configuration->getXml($params['tableId']));
		$response->headers->set('Content-Type', 'application/xml');
		$response->headers->set('Access-Control-Allow-Origin', '*');
		$response->send();
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
		$xmlName = sprintf('%s-%s-%s.xml', date('His'), uniqid(), $params['tableId']);
		$xmlName = $this->_s3Client->uploadString($xmlName, $xml, 'text/xml');

		$tableDefinition = $this->configuration->getTableDefinition($params['tableId']);
		$jobData = array(
			'command' => 'uploadTable',
			'dataset' => !empty($tableDefinition['gdName']) ? $tableDefinition['gdName'] : $tableDefinition['tableId'],
			'createdTime' => date('c', $createdTime),
			'xmlFile' => $xmlName,
			'parameters' => array(
				'tableId' => $params['tableId']
			)
		);
		if (isset($params['incrementalLoad'])) {
			$jobData['parameters']['incrementalLoad'] = $params['incrementalLoad'];
		}
		if (isset($params['sanitize'])) {
			$jobData['parameters']['sanitize'] = $params['sanitize'];
		}
		$jobInfo = $this->_createJob($jobData);
		$this->_queue->enqueueJob($jobInfo);


		if (empty($params['wait'])) {
			return array('job' => (int)$jobInfo['id']);
		} else {
			$this->_waitForJob($jobInfo['id'], $params['writerId']);
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

			try {
				$xml = $this->configuration->getXml($tableInfo['tableId']);
			} catch (WrongConfigurationException $e) {
				throw new WrongConfigurationException(sprintf('Wrong configuration of table \'%s\': %s', $tableInfo['tableId'], $e->getMessage()));
			}
			$xmlName = sprintf('%s-%s-%s.xml', date('His'), uniqid(), $tableInfo['tableId']);
			$xmlName = $this->_s3Client->uploadString($xmlName, $xml, 'text/xml');
			$definition = $this->configuration->getTableDefinition($tableInfo['tableId']);

			$tables[$tableInfo['tableId']] = array(
				'dataset'               => !empty($tableInfo['gdName']) ? $tableInfo['gdName'] : $tableInfo['tableId'],
				'tableId'               => $tableInfo['tableId'],
				'xml'                   => $xmlName,
				'definition'    => $definition['columns']
			);
		}


		// @TODO move the code somewhere else
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

		$batchId = $this->_storageApi->generateId();
		foreach ($sortedTables as $table) {
			$jobData = array(
				'batchId' => $batchId,
				'runId' => $runId,
				'command' => 'uploadTable',
				'dataset' => $table['dataset'],
				'createdTime' => date('c', $createdTime),
				'xmlFile' => $table['xml'],
				'parameters' => array(
					'tableId' => $table['tableId']
				)
			);
			if (isset($params['incrementalLoad'])) {
				$jobData['parameters']['incrementalLoad'] = $params['incrementalLoad'];
			}
			if (isset($params['sanitize'])) {
				$jobData['parameters']['sanitize'] = $params['sanitize'];
			}
			$jobInfo = $this->_createJob($jobData);
			$this->_queue->enqueueJob($jobInfo);
		}

		// Execute reports
		$jobData = array(
			'batchId' => $batchId,
			'runId' => $runId,
			'command' => 'executeReports',
			'createdTime' => date('c', $createdTime)
		);
		$jobInfo = $this->_createJob($jobData);
		$this->_queue->enqueueJob($jobInfo);



		if (empty($params['wait'])) {
			return array('batch' => (int)$batchId);
		} else {
			$this->_waitForBatch($batchId, $params['writerId']);
		}
	}



	/***********************
	 * @section UI support
	 */


	/**
	 * Get visual model
	 * @param $params
	 * @throws Exception\WrongParametersException
	 */
	public function getModel($params)
	{
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		// @TODO move the code somewhere else
		$nodes = array();
		$dateDimensions = array();
		$references = array();
		$datasets = array();
		if (is_array($this->configuration->definedTables) && count($this->configuration->definedTables))
			foreach ($this->configuration->definedTables as $tableInfo) if (!empty($tableInfo['export'])) {
			$datasets[$tableInfo['tableId']] = !empty($tableInfo['gdName']) ? $tableInfo['gdName'] : $tableInfo['tableId'];
			$nodes[] = $tableInfo['tableId'];
			$definition = $this->configuration->getTableDefinition($tableInfo['tableId']);
			foreach ($definition['columns'] as $c) {
				if ($c['type'] == 'DATE' && $c['dateDimension']) {
					$dateDimensions[$tableInfo['tableId']] = $c['dateDimension'];
					if (!in_array($c['dateDimension'], $nodes)) $nodes[] = $c['dateDimension'];
				}
				if ($c['type'] == 'REFERENCE' && $c['schemaReference']) {
					if (!isset($references[$tableInfo['tableId']])) {
						$references[$tableInfo['tableId']] = array();
					}
					$references[$tableInfo['tableId']][] = $c['schemaReference'];
				}
			}
		}

		$datasetIds = array_keys($datasets);
		$result = array('nodes' => array(), 'links' => array());

		foreach ($nodes as $name) {
			$result['nodes'][] = array(
				'name' => isset($datasets[$name]) ? $datasets[$name] : $name,
				'group' => in_array($name, $datasetIds) ? 'dataset' : 'dimension'
			);
		}
		foreach ($dateDimensions as $dataset => $date) {
			$result['links'][] = array(
				'source' => array_search($dataset, $nodes),
				'target' => array_search($date, $nodes),
				'value' => 'dimension'
			);
		}
		foreach ($references as $source => $targets) {
			foreach ($targets as $target) {
				$result['links'][] = array(
					'source' => array_search($source, $nodes),
					'target' => array_search($target, $nodes),
					'value' => 'dataset'
				);
			}
		}

		$response = new Response(json_encode($result));
		$response->headers->set('Content-Type', 'application/json');
		$response->headers->set('Access-Control-Allow-Origin', '*');
		$response->send();
		exit();
	}


	/**
	 *
	 * @param $params
	 * @return array
	 * @throws Exception\WrongParametersException
	 */
	public function getTables($params)
	{
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		if (isset($params['tableId'])) {
			// Table detail
			if (!in_array($params['tableId'], $this->configuration->getOutputTables())) {
				throw new WrongParametersException(sprintf("Table '%s' does not exist", $params['tableId']));
			}
			if (!isset($this->configuration->definedTables[$params['tableId']])) {
				$this->configuration->createTableDefinition($params['tableId']);
			}

			return array('table' => $this->configuration->getTableForApi($params['tableId']));
		} elseif (isset($params['referenceable'])) {
			return array('tables' => $this->configuration->getReferenceableTables());
		} else {
			return array('tables' => $this->configuration->getTables());
		}
	}

	/**
	 *
	 * @param $params
	 * @return array
	 * @throws Exception\WrongParametersException
	 */
	public function postTables($params)
	{
		if (empty($params['tableId'])) {
			throw new WrongParametersException("Parameter 'tableId' is missing");
		}
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}
		if (!in_array($params['tableId'], $this->configuration->getOutputTables())) {
			throw new WrongParametersException(sprintf("Table '%s' does not exist", $params['tableId']));
		}

		if (!isset($this->configuration->definedTables[$params['tableId']])) {
			$this->configuration->createTableDefinition($params['tableId']);
		}

		if (isset($params['column'])) {
			$params['column'] = trim($params['column']);

			// Column detail
			$sourceTableInfo = $this->configuration->getTable($params['tableId']);
			if (!in_array($params['column'], $sourceTableInfo['columns'])) {
				throw new WrongParametersException(sprintf("Table '%s' does not exist", $params['tableId']));
			}

			$values = array('name' => $params['column']);
			foreach ($params as $key => $value) if (in_array($key, array('gdName', 'type', 'dataType', 'dataTypeSize',
				'schemaReference', 'reference', 'format', 'dateDimension', 'sortLabel', 'sortOrder'))) {
				$values[$key] = $value;
							}
			if (count($values) > 1) {
				$this->configuration->saveColumnDefinition($params['tableId'], $values);
				$this->configuration->setTableAttribute($params['tableId'], 'lastChangeDate', date('c'));
			}
		} else {
			// Table detail
			$this->configuration->setTableAttribute($params['tableId'], 'lastChangeDate', date('c'));
			foreach ($params as $key => $value) if (in_array($key, array('gdName', 'export', 'lastChangeDate', 'lastExportDate', 'sanitize', 'incrementalLoad'))) {
				$this->configuration->setTableAttribute($params['tableId'], $key, $value);
			}
		}

		return array();
	}


	/**
	 * Reset export status of all datasets and dimensions
	 * @param $params
	 * @return array
	 * @throws Exception\WrongParametersException
	 */
	public function postResetExport($params)
	{
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		foreach ($this->configuration->definedTables as $table) if (!empty($table['lastExportDate'])) {
			$this->configuration->setTableAttribute($table['tableId'], 'lastExportDate', '');
		}
		foreach ($this->configuration->getDateDimensions() as $dimension) if (!empty($dimension['lastExportDate'])) {
			$this->configuration->setDateDimensionAttribute($dimension['name'], 'lastExportDate', '');
		}

		return array();
	}


	/**
	 *
	 * @param $params
	 * @return array
	 * @throws Exception\WrongParametersException
	 */
	public function getDateDimensions($params)
	{
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		return array('dimensions' => $this->configuration->getDateDimensions(isset($params['usage'])));
	}


	/**
	 *
	 * @param $params
	 * @return array
	 * @throws Exception\WrongParametersException
	 */
	public function deleteDateDimensions($params)
	{
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}
		if (!isset($params['name'])) {
			throw new WrongParametersException("Parameter 'name' is missing");
		}

		$dimensions = $this->configuration->getDateDimensions();
		if (isset($dimensions[$params['name']])) {
			$this->configuration->deleteDateDimension($params['name']);
			return array();
		} else {
			throw new WrongParametersException(sprintf("Dimension '%s' does not exist", $params['name']));
		}
	}


	/**
	 *
	 * @param $params
	 * @return array
	 * @throws Exception\WrongParametersException
	 */
	public function postDateDimensions($params)
	{
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		if (!isset($params['name'])) {
			throw new WrongParametersException("Parameter 'name' is missing");
		}
		$params['name'] = trim($params['name']);

		$dimensions = $this->configuration->getDateDimensions();
		if (isset($dimensions[$params['name']])) {
			// Update
			if (isset($params['includeTime'])) {
				$this->configuration->setDateDimensionAttribute($params['name'], 'includeTime', $params['includeTime']);
			}
			if (isset($params['lastExportDate'])) {
				$this->configuration->setDateDimensionAttribute($params['name'], 'lastExportDate', $params['lastExportDate']);
			}
		} else {
			// Create
			$this->configuration->addDateDimension($params['name'], !empty($params['includeTime']));
		}

		return array();
	}



	/***********************
	 * @section Jobs
	 */

	/**
	 * Get Jobs
	 * @param $params
	 * @throws Exception\WrongParametersException
	 * @return array
	 */
	public function getJobs($params)
	{
		$this->_init($params);
		if (!$this->configuration->bucketId) {
			throw new WrongParametersException(sprintf("Writer '%s' does not exist", $params['writerId']));
		}

		if (empty($params['jobId'])) {
			$days = isset($params['days']) ? $params['days'] : 7;
			$jobs = $this->sharedConfig->fetchJobs($this->configuration->projectId, $params['writerId'], $days);

			$result = array();
			foreach ($jobs as $job) {
				$result[] = $this->sharedConfig->jobToApiResponse($job, $this->_s3Client);
			}

			return array('jobs' => $result);
		} else {
			if (is_array($params['jobId'])) {
				throw new WrongParametersException("Parameter 'jobId' has to be a number");
			}
			$job = $this->sharedConfig->fetchJob($params['jobId'], $this->configuration->writerId, $this->configuration->projectId);
			if (!$job) {
				throw new WrongParametersException(sprintf("Job '%d' does not belong to writer '%s'", $params['jobId'], $this->configuration->writerId));
			}

			if (isset($params['detail']) && $params['detail'] == 'csv') {
				$result = json_decode($job['result'], true);
				if (isset($result['csvFile']) && file_exists($result['csvFile'])) {

					$response = new StreamedResponse(function() use($result) {
						$linesCount = !empty($params['limit']) ? $params['limit'] : -1;
						$handle = fopen($result['csvFile'], 'r');
						if ($handle === false) {
							return false;
						}
						while (($buffer = fgets($handle)) !== false && $linesCount != 0) {
							print $buffer;
							$linesCount--;
						}
						fclose($handle);
					});
					$response->headers->set('Content-Disposition', $response->headers->makeDisposition(ResponseHeaderBag::DISPOSITION_ATTACHMENT, $params['jobId'] . '.csv'));
					$response->headers->set('Content-Length', filesize($result['csvFile']));
					$response->headers->set('Content-Type', 'text/csv');
					$response->headers->set('Access-Control-Allow-Origin', '*');
					$response->send();
					exit();

				} else {
					throw new WrongParametersException("There is no csvFile for this job");
				}
			} else {
				$job = $this->sharedConfig->jobToApiResponse($job, $this->_s3Client);
				return array('job' => $job);
			}
		}
	}

	/**
	 * Cancel waiting jobs
	 * @param $params
	 * @return array
	 * @throws Exception\WrongParametersException
	 */
	public function postCancelJobs($params)
	{
		$this->_init($params);
		if (!isset($params['writerId'])) {
			throw new WrongParametersException('Missing parameter \'writerId\'');
		}

		$jobs = $this->_queue->clearQueue($this->configuration->projectId . "-" . $this->configuration->writerId);

		// Cancel only waiting (to skip processing jobs)
		foreach ($this->sharedConfig->fetchJobs($this->configuration->projectId, $this->configuration->writerId) as $job) {
			if (in_array($job['id'], $jobs) && $job['status'] == 'waiting') {
				$this->sharedConfig->saveJob($job['id'], array('status' => 'cancelled'));
			}
		}
		return array();
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
		if (empty($params['batchId'])) {
			throw new WrongParametersException("Parameter 'batchId' is missing");
		}

		$data = array(
			'batchId' => (int)$params['batchId'],
			'createdTime' => date('c'),
			'startTime' => date('c'),
			'endTime' => null,
			'status' => null,
			'jobs' => array(),
			'result' => null,
			'log' => null
		);
		$cancelledJobs = 0;
		$waitingJobs = 0;
		$processingJobs = 0;
		$errorJobs = 0;
		$successJobs = 0;
		foreach ($this->sharedConfig->fetchBatch($params['batchId']) as $job) {
			$job = $this->sharedConfig->jobToApiResponse($job, $this->_s3Client);

			if ($job['projectId'] != $this->configuration->projectId || $job['writerId'] != $this->configuration->writerId) {
				throw new WrongParametersException(sprintf("Job '%d' does not belong to writer '%s'", $params['batchId'], $this->configuration->writerId));
			}

			if ($job['createdTime'] < $data['createdTime']) $data['createdTime'] = $job['createdTime'];
			if ($job['startTime'] < $data['startTime']) $data['startTime'] = $job['startTime'];
			if ($job['endTime'] > $data['endTime']) $data['endTime'] = $job['endTime'];
			$data['jobs'][] = (int)$job['id'];
			if ($job['status'] == 'waiting') $waitingJobs++;
			elseif ($job['status'] == 'processing') $processingJobs++;
			elseif ($job['status'] == 'cancelled') $cancelledJobs++;
			elseif ($job['status'] == 'error') {
				$errorJobs++;
				$data['result'] = $job['result'];
			}
			else $successJobs++;
		}

		if ($cancelledJobs > 0) $data['status'] = 'cancelled';
		elseif ($processingJobs > 0) $data['status'] = 'processing';
		elseif ($waitingJobs > 0) $data['status'] = 'waiting';
		elseif ($errorJobs > 0) $data['status'] = 'error';
		else $data['status'] = 'success';

		return array('batch' => $data);
	}




	private function _createJob($params)
	{
		$jobId = $this->_storageApi->generateId();
		if (!isset($params['batchId'])) {
			$params['batchId'] = $jobId;
		}

		$jobInfo = array(
			'id' => $jobId,
			'runId' => $this->_storageApi->getRunId(),
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
			'log' => null,
			'projectIdWriterId' => $this->configuration->projectId . '.' . $this->configuration->writerId
		);
		$jobInfo = array_merge($jobInfo, $params);
		$this->sharedConfig->saveJob($jobId, $jobInfo);

		$message = "Writer job $jobId created manually";
		$results = array('jobId' => $jobId);
		$this->sharedConfig->logEvent($this->configuration->writerId, $jobInfo['runId'], $message, $params, $results);

		$this->_log->log(Logger::INFO, $message, array(
			'token' => $this->_storageApi->getLogData(),
			'configurationId' => $this->configuration->writerId,
			'runId' => $jobInfo['runId'],
			'params' => $params,
			'results' => $results
		));

		return $jobInfo;
	}


	protected function _waitForJob($jobId, $writerId)
	{
		$jobFinished = false;
		$i = 1;
		do {
			$jobInfo = $this->getJobs(array('jobId' => $jobId, 'writerId' => $writerId));
			if (isset($jobInfo['job']['status']) && !in_array($jobInfo['job']['status'], array('waiting', 'processing'))) {
				$jobFinished = true;
			}
			if (!$jobFinished) sleep($i * 10);
			$i++;
		} while(!$jobFinished);

		if ($jobInfo['job']['status'] == 'success') {
			return $jobInfo;
		} else {
			$e = new JobProcessException('Job processing failed');
			$e->setData(array('result' => $jobInfo['job']['result'], 'log' => $jobInfo['job']['log']));
			throw $e;
		}
	}

	protected function _waitForBatch($batchId, $writerId)
	{
		$jobsFinished = false;
		$i = 1;
		do {
			$jobsInfo = $this->getBatch(array('batchId' => $batchId, 'writerId' => $writerId));
			if (isset($jobsInfo['batch']['status']) && !in_array($jobsInfo['batch']['status'], array('waiting', 'processing'))) {
				$jobsFinished = true;
			}
			if (!$jobsFinished) sleep($i * 10);
			$i++;
		} while(!$jobsFinished);

		if ($jobsInfo['batch']['status'] == 'success') {
			return $jobsInfo;
		} else {
			$e = new JobProcessException('Batch processing failed');
			$e->setData(array('result' => $jobsInfo['batch']['result'], 'log' => $jobsInfo['batch']['log']));
			throw $e;
		}
	}

}
