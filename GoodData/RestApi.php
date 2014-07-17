<?php
/**
 *
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-19
 *
 */

namespace Keboola\GoodDataWriter\GoodData;

use Guzzle\Http\Client,
	Guzzle\Http\Exception\ServerErrorResponseException,
	Guzzle\Http\Exception\ClientErrorResponseException,
	Guzzle\Http\Message\Header;
use Guzzle\Http\Curl\CurlHandle;
use Guzzle\Http\Exception\CurlException;
use Guzzle\Stream\PhpStreamRequestFactory;
use Guzzle\Http\Message\RequestInterface;
use Keboola\GoodDataWriter\Exception\JobProcessException;
use Keboola\GoodDataWriter\Service\EventLogger;
use Monolog\Logger;
use stdClass;
use Syrup\ComponentBundle\Filesystem\TempService;
use Syrup\ComponentBundle\Filesystem\TempServiceFactory;

class RestApiException extends \Exception
{
	private $details;

	public function __construct($message, $details = null, $code = 0, \Exception $previous = null)
	{
		if ($details) {
			$this->setDetails($details);
		}

		parent::__construct($message, $code, $previous);
	}

	public function getDetails()
	{
		$result = array(
			'error' => $this->getMessage(),
			'source' => 'Rest API'
		);
		if (count($this->details)) {
			$result['details'] = $this->details;
		}
		return $result;
	}

	public function setDetails($details)
	{
		if (!is_array($details)) {
			$decode = json_decode($details, true);
			$details = $decode ? $decode : array($details);
		}

		$details = RestApi::parseError($details);
		foreach ($details as &$detail) {
			$detail = RestApi::parseError($detail);
		}

		$this->details = $details;
	}

}

class UserAlreadyExistsException extends RestApiException
{

}



class RestApi
{

	/**
	 * Number of retries for one API call
	 */
	const RETRIES_COUNT = 5;
	/**
	 * Back off time before retrying API call
	 */
	const BACKOFF_INTERVAL = 60;
	const WAIT_INTERVAL = 10;

	const API_URL = 'https://na1.secure.gooddata.com';
	const DEFAULT_BACKEND_URL = 'na1.secure.gooddata.com';

	const USER_ROLE_ADMIN = 'adminRole';
	const USER_ROLE_EDITOR = 'editorRole';
	const USER_ROLE_READ_ONLY = 'readOnlyUserRole';
	const USER_ROLE_DASHBOARD_ONLY = 'dashboardOnlyRole';

	public static $userRoles = array(
		'admin' => self::USER_ROLE_ADMIN,
		'editor' => self::USER_ROLE_EDITOR,
		'readOnly' => self::USER_ROLE_READ_ONLY,
		'dashboardOnly' => self::USER_ROLE_DASHBOARD_ONLY
	);

	/**
	 * @var Client
	 */
	protected $client;
	/**
	 * @var Model
	 */
	protected $model;
	/**
	 * @var Logger
	 */
	protected $logger;
	/**
	 * @var Logger
	 */
	private $usageLogger;
	/**
	 * @var TempService
	 */
	private $tempService;

	/**
	 * @var \SplFileObject
	 */
	private $logFile;
	/**
	 * @var EventLogger
	 */
	private $eventLogger;

	protected $authSst;
	protected $authTt;

	private $jobId;
	private $runId;
	private $clearFromLog;

	private $apiUrl;
	private $username;
	private $password;



	public function __construct(Model $model, Logger $logger, Logger $usageLogger, TempServiceFactory $tempServiceFactory)
	{
		$this->model = $model;
		$this->logger = $logger;
		$this->usageLogger = $usageLogger;

		$this->client = new Client(self::API_URL, array(
			'curl.options' => array(
				CURLOPT_CONNECTTIMEOUT => 600,
				CURLOPT_TIMEOUT => 600
			)
		));
		$this->client->setSslVerification(false);
		$this->client->setDefaultOption('headers', array(
			'accept' => 'application/json',
			'content-type' => 'application/json; charset=utf-8'
		));

		$this->tempService = $tempServiceFactory->get('gooddata-writer');
		$this->clearFromLog = array();

		$this->initLog();
	}

	public function setJobId($jobId)
	{
		$this->jobId = $jobId;
	}

	public function setRunId($runId)
	{
		$this->runId = $runId;
	}

	public function setBaseUrl($url)
	{
		if (substr($url, 0, 7) == 'http://') {
			$baseUrl = 'https://' . substr($url, 7);
		} elseif (substr($url, 0, 8) == 'https://') {
			$baseUrl = $url;
		} else {
			$baseUrl = 'https://' . $url;
		}
		$this->client->setBaseUrl($baseUrl);
	}

	public function setEventLogger(EventLogger $eventLogger)
	{
		$this->eventLogger = $eventLogger;
	}

	public function getApiUrl()
	{
		return $this->apiUrl;
	}

	public static function parseError($message)
	{
		if (isset($message['error']) && isset($message['error']['parameters']) && isset($message['error']['message'])) {
			$message['error']['message'] = vsprintf($message['error']['message'], $message['error']['parameters']);
			unset($message['error']['parameters']);
		}
		return $message;
	}

	public static function getUserUri($uid)
	{
		return '/gdc/account/profile/' . $uid;
	}

	public static function getUserId($uri)
	{
		return substr($uri, strrpos($uri, '/')+1);
	}

	/**
	 * Get project info
	 *
	 * @param $pid
	 * @throws RestApiException|\Exception
	 * @return array
	 */
	public function getProject($pid)
	{
		return $this->get(sprintf('/gdc/projects/%s', $pid));
	}

	/**
	 * @param $pid
	 * @return array
	 */
	public function getDataSets($pid)
	{
		$result = array();
		$call = $this->get(sprintf('/gdc/md/%s/data/sets', $pid));
		foreach ($call['dataSetsInfo']['sets'] as $r) {
			$result[$r['meta']['identifier']] = array(
				'id' => $r['meta']['identifier'],
				'title' => $r['meta']['title'],
				'lastChangeDate' => !empty($r['meta']['updated']) ? $r['meta']['updated'] : null
			);
		}
		return $result;
	}

	/**
	 * Get user info
	 *
	 * @param $uid
	 * @throws RestApiException|\Exception
	 * @return array
	 */
	public function getUser($uid)
	{
		return $this->get(sprintf('/gdc/account/profile/%s', $uid));
	}

	/**
	 * Get object info
	 *
	 * @param $uri
	 * @throws RestApiException|\Exception
	 * @return array
	 */
	public function get($uri)
	{
		try {
			$result = $this->jsonRequest($uri, 'GET', array(), array(), false);
			return $result;
		} catch (RestApiException $e) {
			$errorJson = json_decode($e->getMessage(), true);
			if ($errorJson) {
				if (isset($errorJson['error']['errorClass'])) {
					switch ($errorJson['error']['errorClass']) {
						case 'GDC::Exception::Forbidden':
							throw new RestApiException(sprintf('Access to uri %s denied', $uri));
							break;
						case 'GDC::Exception::NotFound':
							throw new RestApiException(sprintf('Uri %s does not exist', $uri));
							break;
					}
				}
			}
			throw $e;
		}

	}


	/**
	 * Create project
	 */
	public function createProject($name, $authToken, $description = null)
	{
		$uri = '/gdc/projects';
		$params = array(
			'project' => array(
				'content' => array(
					'guidedNavigation' => 1,
					'driver' => 'Pg',
					'authorizationToken' => $authToken
				),
				'meta' => array(
					'title' => $name
				)
			)
		);
		if ($description) {
			$params['project']['meta']['summary'] = $description;
		}
		$result = $this->jsonRequest($uri, 'POST', $params);

		if (empty($result['uri']) || strpos($result['uri'], '/gdc/projects/') === false) {
			$this->logAlert('createProject() failed', array(
				'uri' => $uri,
				'params' => $params,
				'result' => $result
			));
			throw new RestApiException('Create project call failed');
		}

		$projectUri = $result['uri'];
		$projectId = substr($projectUri, 14);

		// Wait until project is ready
		$repeat = true;
		$i = 1;
		do {
			sleep(self::WAIT_INTERVAL * ($i + 1));

			$result = $this->jsonRequest($projectUri);
			if (isset($result['project']['content']['state'])) {
				if ($result['project']['content']['state'] == 'ENABLED') {
					$repeat = false;
				}
			} else {
				$this->logAlert('createProject() failed', array(
					'uri' => $projectUri,
					'result' => $result
				));
				throw new RestApiException('Create project call failed');
			}

			$i++;
		} while ($repeat);

		return $projectId;
	}

	/**
	 * Drop project
	 * @param $pid
	 * @return string
	 */
	public function dropProject($pid)
	{
		$uri = '/gdc/projects/' . $pid;
		return $this->jsonRequest($uri, 'DELETE');
	}


	/**
	 * Clone project from other project
	 * @param $pidSource
	 * @param $pidTarget
	 * @param $includeData
	 * @param $includeUsers
	 * @throws RestApiException
	 * @return bool
	 */
	public function cloneProject($pidSource, $pidTarget, $includeData, $includeUsers)
	{
		$uri = sprintf('/gdc/md/%s/maintenance/export', $pidSource);
		$params = array(
			'exportProject' => array(
				'exportUsers' => $includeUsers,
				'exportData' => $includeData
			)
		);
		$result = $this->jsonRequest($uri, 'POST', $params);
		if (empty($result['exportArtifact']['token']) || empty($result['exportArtifact']['status']['uri'])) {
			$this->logAlert('cloneProject() export failed', array(
				'uri' => $uri,
				'params' => $params,
				'result' => $result
			));
			throw new RestApiException('Clone export failed');
		}

		$this->waitForTask($result['exportArtifact']['status']['uri']);

		$result = $this->jsonRequest(sprintf('/gdc/md/%s/maintenance/import', $pidTarget), 'POST', array(
			'importProject' => array(
				'token' => $result['exportArtifact']['token']
			)
		));
		if (empty($result['uri'])) {
			$this->logAlert('cloneProject() import failed', array(
				'uri' => $uri,
				'params' => $params,
				'result' => $result
			));
			throw new RestApiException('Clone import failed');
		}

		$this->waitForTask($result['uri']);
	}

	public function validateProject($pid, $validate = array('pdm'))
	{
		$result = $this->jsonRequest(sprintf('/gdc/md/%s/validate', $pid), 'POST', array('validateProject' => $validate));
		if (!isset($result['asyncTask']['link']['poll'])) {
			throw new RestApiException('Project validation failed');
		}

		// Wait until validation is ready
		$repeat = true;
		$i = 1;
		do {
			sleep(self::WAIT_INTERVAL * ($i + 1));

			$result = $this->jsonRequest($result['asyncTask']['link']['poll']);
			if (isset($result['projectValidateResult'])) {
				return $result['projectValidateResult'];
			}

			$i++;
		} while ($repeat);

		return false;
	}

	public function hasAccessToProject($pid)
	{
		$accountInfo = $this->jsonRequest('/gdc/account/profile/current');
		if (isset($accountInfo['accountSetting']['links']['projects'])) {
			$projects = $this->jsonRequest($accountInfo['accountSetting']['links']['projects']);
			foreach($projects['projects'] as $p) {
				if ($p['project']['links']['self'] == '/gdc/projects/' . $pid) {
					return true;
				}
			}
		}
		return false;
	}


	/**
	 * Creates user in the domain
	 *
	 * @param $domain
	 * @param $login
	 * @param $password
	 * @param $firstName
	 * @param $lastName
	 * @param $ssoProvider
	 * @throws \Exception|\Keboola\GoodDataWriter\GoodData\RestApiException
	 * @throws \Keboola\GoodDataWriter\GoodData\RestApiException
	 * @return string
	 */
	public function createUser($domain, $login, $password, $firstName, $lastName, $ssoProvider)
	{
		$this->clearFromLog[] = $password;

		$uri = sprintf('/gdc/account/domains/%s/users', $domain);
		$params = array(
			'accountSetting' => array(
				'login' => strtolower($login),
				'email' => strtolower($login),
				'password' => $password,
				'verifyPassword' => $password,
				'firstName' => $firstName,
				'lastName' => $lastName,
				'ssoProvider' => $ssoProvider
			),
		);

		try {
			$result = $this->jsonRequest($uri, 'POST', $params);
		} catch (RestApiException $e) {
			// User exists?
			$userId = $this->userId($login, $domain);
			if ($userId) {
				throw new UserAlreadyExistsException($userId);
			} else {
				$details = $e->getDetails();
				if (isset($details['details']['error']['errorClass']) && strpos($details['details']['error']['errorClass'], 'LoginNameAlreadyRegisteredException') !== null) {
					throw new RestApiException('Account already exists in another domain', $e->getDetails(), $e->getCode(), $e);
				} else {
					throw $e;
				}
			}
		}

		if (isset($result['uri'])) {
			if (substr($result['uri'], 0, 21) == '/gdc/account/profile/') {
				return substr($result['uri'], 21);
			} else {
				$this->logAlert('createUser() has wrong result', array(
					'uri' => $uri,
					'params' => $params,
					'result' => $result
				));
			}
		} else {
			$this->logAlert('createUser() failed', array(
				'uri' => $uri,
				'params' => $params,
				'result' => $result
			));
			throw new RestApiException('Create user failed');
		}

		return false;
	}

	/**
	 * Drop user
	 * @param $uid
	 * @return string
	 */
	public function dropUser($uid)
	{
		return $this->jsonRequest('/gdc/account/profile/' . $uid, 'DELETE');
	}

	/**
	 * Retrieves user's uri
	 *
	 * @param $email
	 * @param $domain
	 * @return array|bool
	 */
	public function userId($email, $domain)
	{
		foreach($this->usersInDomain($domain) as $user) {
			if (!empty($user['accountSetting']['login']) && strtolower($user['accountSetting']['login']) == strtolower($email)) {
				if (!empty($user['accountSetting']['links']['self'])) {
					if (substr($user['accountSetting']['links']['self'], 0, 21) == '/gdc/account/profile/') {
						return substr($user['accountSetting']['links']['self'], 21);
					} else {
						$this->logAlert('userId() has wrong result', array(
							'email' => $email,
							'domain' => $domain,
							'result' => $user
						));
					}
				}
				return false;
			}
		}
		return false;
	}

	/**
	 * Retrieves list of all users in the domain
	 *
	 * @param $domain
	 * @return array
	 */
	public function usersInDomain($domain)
	{
		$users = array();

		// first page
		$result = $this->jsonRequest(sprintf('/gdc/account/domains/%s/users', $domain), 'GET', array(), array(), false);

		if (isset($result['accountSettings']['items']))
			$users = array_merge($users, $result['accountSettings']['items']);

		// next pages
		while (isset($result['accountSettings']['paging']['next'])) {
			$result = $this->jsonRequest($result['accountSettings']['paging']['next'], 'GET', array(), array(), false);

			if (isset($result['accountSettings']['items']))
				$users = array_merge($users, $result['accountSettings']['items']);
		}

		return $users;
	}

	/**
	 * Retrieves list of all users in the project
	 *
	 * @param $pid
	 * @return array
	 */
	public function usersInProject($pid)
	{
		$users = array();

		// first page
		$result = $this->jsonRequest(sprintf('/gdc/projects/%s/users', $pid));

		//@TODO paging?

		if (isset($result['users']))
			$users = array_merge($users, $result['users']);

		return $users;
	}

	/**
	 * Retrieve userId from project users
	 *
	 * @param $email
	 * @param $pid
	 * @return bool|string
	 */
	public function userIdByProject($email, $pid)
	{
		foreach ($this->usersInProject($pid) as $user) {
			if (!empty($user['user']['content']['login']) && strtolower($user['user']['content']['login']) == strtolower($email)) {
				if (!empty($user['user']['links']['self'])) {
					if (substr($user['user']['links']['self'], 0, 21) == '/gdc/account/profile/') {
						return substr($user['user']['links']['self'], 21);
					} else {
						$this->logAlert('userId() has wrong result', array(
							'email' => $email,
							'pid' => $pid,
							'result' => $user
						));
					}
				}
				return false;
			}
		}
		return false;
	}

	/**
	 * Adds user to the project
	 *
	 * @param $userId
	 * @param $pid
	 * @param string $role
	 * @throws \Exception|\Guzzle\Http\Exception\ClientErrorResponseException
	 * @throws RestApiException
	 */
	public function addUserToProject($userId, $pid, $role = self::USER_ROLE_ADMIN)
	{
		$projectRoleUri = $this->getRoleId($role, $pid);

		$uri = sprintf('/gdc/projects/%s/users', $pid);
		$params = array(
			'user' => array(
				'content' => array(
					'status' => 'ENABLED',
					'userRoles' => array($projectRoleUri)
				),
				'links' => array(
					'self' => '/gdc/account/profile/' . $userId
				)
			)
		);
		$result = $this->jsonRequest($uri, 'POST', $params);

		if ((isset($result['projectUsersUpdateResult']['successful']) && count($result['projectUsersUpdateResult']['successful']))
			|| (isset($result['projectUsersUpdateResult']['failed']) && !count($result['projectUsersUpdateResult']['failed']))) {
			// SUCCESS
			// Sometimes API does not return
		} else {
			$errors = array();
			if (isset($result['projectUsersUpdateResult']['failed'])) foreach ($result['projectUsersUpdateResult']['failed'] as $f) {
				$errors[] = $f['message'];
			}
			throw new RestApiException('Error in addition to project ' . implode('; ', $errors));
		}
	}

	/**
	 * Remove user from the project
	 *
	 * @param $userId
	 * @param $pid
	 * @throws RestApiException
	 * @internal param string $role
	 */
	public function removeUserFromProject($userId, $pid)
	{
		// GD BUG fix - adding user roles
		$rolesUri = sprintf('/gdc/projects/%s/users/%s/roles', $pid, $userId);
		$result = $this->get($rolesUri);
		if ((isset($result['associatedRoles']['roles']) && count($result['associatedRoles']['roles']))) {
			// SUCCESS
		} else {
			$this->logAlert('removeUserFromProject() has not remove user from project - could not load user roles', array(
				'uri' => $rolesUri,
				'result' => $result
			));
			throw new RestApiException('Error removing from project');
		}

		$uri = sprintf('/gdc/projects/%s/users', $pid);
		$params = array(
			'user' => array(
				'content' => array(
					'status' => 'DISABLED',
					'userRoles' => $result['associatedRoles']['roles'],
				),
				'links' => array(
					'self' => '/gdc/account/profile/' . $userId
				)
			)
		);
		$result = $this->jsonRequest($uri, 'POST', $params);

		if ((isset($result['projectUsersUpdateResult']['successful']) && count($result['projectUsersUpdateResult']['successful']))
			|| (isset($result['projectUsersUpdateResult']['failed']) && !count($result['projectUsersUpdateResult']['failed']))) {
			// SUCCESS
			// Sometimes API does not return
		} else {
			$this->logAlert('removeUserFromProject() has not remove user from project', array(
				'uri' => $uri,
				'params' => $params,
				'result' => $result
			));
			throw new RestApiException('Error removing from project');
		}
	}

	/**
	 * Invites user to the project
	 *
	 * @param $email
	 * @param $pid
	 * @param string $role
	 * @throws \Exception|\Guzzle\Http\Exception\ClientErrorResponseException
	 * @throws RestApiException
	 */
	public function inviteUserToProject($email, $pid, $role = self::USER_ROLE_ADMIN)
	{
		$projectRoleUri = $this->getRoleId($role, $pid);

		$uri = sprintf('/gdc/projects/%s/invitations', $pid);
		$params = array(
			'invitations' => array(
				array(
					'invitation' => array(
						'content' => array(
							'email' => $email,
							'role' => $projectRoleUri
						)
					)
				)
			)
		);
		$result = $this->jsonRequest($uri, 'POST', $params);

		if (isset($result['createdInvitations']['uri']) && count($result['createdInvitations']['uri'])) {
			return current($result['createdInvitations']['uri']);
		} else {
			$this->logAlert('inviteUserToProject() has not invited user to project', array(
				'uri' => $uri,
				'params' => $params,
				'result' => $result
			));
			throw new RestApiException('Error in invitation to project');
		}
	}

	public function disableUserInProject($userUri, $pid)
	{
		$projectRoleUri = $this->getRoleId('editorRole', $pid);

		$uri = sprintf('/gdc/projects/%s/users', $pid);
		$params = array(
			'user' => array(
				'content' => array(
					'status' => 'DISABLED',
					'userRoles' => array($projectRoleUri)
				),
				'links' => array(
					'self' => $userUri
				)
			)
		);
		$this->jsonRequest($uri, 'POST', $params);
	}

	public function getRoleId($role, $pid)
	{
		$rolesUri = sprintf('/gdc/projects/%s/roles', $pid);
		$rolesResult = $this->jsonRequest($rolesUri);
		if (isset($rolesResult['projectRoles']['roles'])) {
			foreach($rolesResult['projectRoles']['roles'] as $roleUri) {
				$roleResult = $this->jsonRequest($roleUri);
				if (isset($roleResult['projectRole']['meta']['identifier']) && $roleResult['projectRole']['meta']['identifier'] == $role) {
					return $roleUri;
				}
			}
		} else {
			$this->logAlert('inviteUserToProject() has bad response', array(
				'uri' => $rolesUri,
				'result' => $rolesResult
			));
			throw new RestApiException('Roles in project could not be fetched');
		}

		$this->logAlert('inviteUserToProject() has not found role in project', array(
			'role' => $role,
			'result' => $rolesResult
		));
		throw new RestApiException('Role in project not found');
	}

	public function getUserRolesInProject($username, $pid)
	{
		$user = false;
		foreach ($this->usersInProject($pid) as $user) {
			if ($user['user']['content']['login'] == $username) {
				$user = $user['user']['content'];
				break;
			}
		}

		if (!$user) {
			throw new RestApiException('User is not in the project');
		}

		$roles = array();
		foreach ($user['userRoles'] as $roleUri) {
			$role = $this->get($roleUri);
			$roleKey = array_search($role['projectRole']['meta']['identifier'], self::$userRoles);
			$roles[] = $roleKey? $roleKey : $role['projectRole']['meta']['title'];
		}

		return $roles;
	}

	/**
	 * Cancel User Invitation to Project
	 *
	 */
	public function cancelInviteUserToProject($email, $pid)
	{
		$invitationsUri = sprintf('/gdc/projects/%s/invitations', $pid);

		$invitationsResult = $this->jsonRequest($invitationsUri, 'GET');

		if (isset($invitationsResult['invitations'])) {
			foreach ($invitationsResult['invitations'] AS $invitationData) {
				$invitationEmail = $invitationData['invitation']['content']['email'];
				$invitationStatus = $invitationData['invitation']['content']['status'];
				$invitationUri = $invitationData['invitation']['links']['self'];

				if (strtolower($invitationEmail) != strtolower($email))
					continue;

				if ($invitationStatus == 'CANCELED')
					continue;

				$this->jsonRequest($invitationUri, 'DELETE');
			}
		}
	}

	/**
	 * Run load data task
	 */
	public function loadData($pid, $dirName)
	{
		$uri = sprintf('/gdc/md/%s/etl/pull', $pid);
		$result = $this->jsonRequest($uri, 'POST', array('pullIntegration' => $dirName));

		if (isset($result['pullTask']['uri'])) {

			$try = 1;
			do {
				sleep(10 * $try);
				$taskResponse = $this->jsonRequest($result['pullTask']['uri']);

				if (!isset($taskResponse['taskStatus'])) {
					$this->logAlert('loadData() has bad response', array(
						'uri' => $result['pullTask']['uri'],
						'result' => $taskResponse
					));
					throw new RestApiException('ETL task could not be checked');
				}

				$try++;
			} while (in_array($taskResponse['taskStatus'], array('PREPARED', 'RUNNING')));

			if ($taskResponse['taskStatus'] == 'ERROR' || $taskResponse['taskStatus'] == 'WARNING') {
				// Find upload message
				$taskId = substr($result['pullTask']['uri'], strrpos($result['pullTask']['uri'], '/')+1);
				$upload = $this->get(sprintf('/gdc/md/%s/data/upload/%s', $pid, $taskId));
				throw new RestApiException(isset($upload['dataUpload']['msg'])? $upload['dataUpload']['msg'] : 'Data load failed');
			}

			return $taskResponse;

		} else {
			$this->logAlert('loadData() has bad response', array(
				'uri' => $uri,
				'result' => $result
			));
			throw new RestApiException('ETL task could not be started');
		}
	}



	public function updateDataSet($pid, $definition)
	{
		$dataSetModel = $this->model->getLDM($definition);
		$model = $this->getProjectModel($pid);

		if (!isset($model['projectModel']['datasets']))
			$model['projectModel']['datasets'] = array();
		$dataSetFound = false;
		foreach ($model['projectModel']['datasets'] as &$dataSet) {
			if ($dataSet['dataset']['title'] == $definition['name']) {
				$dataSetFound = true;
				$dataSet['dataset'] = $dataSetModel;
				break;
			}
		}

		if (!$dataSetFound) {
			$model['projectModel']['datasets'][] = array('dataset' => $dataSetModel);
		}

		$update = $this->generateUpdateProjectMaql($pid, $model);
		if (count($update['lessDestructiveMaql'])) foreach($update['lessDestructiveMaql'] as $i => $m) {

			try {
				$this->executeMaql($pid, $m);
			} catch (RestApiException $e) {
				$this->executeMaql($pid, $update['moreDestructiveMaql'][$i]);
			}

			return $update['description'];
		}
		return false;
	}

	public function generateUpdateProjectMaql($pid, $model)
	{
		try {
			$uri = sprintf('/gdc/projects/%s/model/diff', $pid);
			$result = $this->jsonRequest($uri, 'POST', array('diffRequest' => array('targetModel' => $model)));

			if (isset($result['asyncTask']['link']['poll'])) {

				$try = 1;
				do {
					sleep(10 * $try);
					$taskResponse = $this->jsonRequest($result['asyncTask']['link']['poll']);

					if (!isset($taskResponse['asyncTask']['link']['poll'])) {
						if (isset($taskResponse['projectModelDiff']['updateScripts'])) {
							$cascadeDrops = array();
							$noCascadeDrops = array();
							foreach($taskResponse['projectModelDiff']['updateScripts'] as $updateScript) {
								if (!$updateScript['updateScript']['preserveData'] && !$updateScript['updateScript']['cascadeDrops']) {
									$noCascadeDrops = $updateScript['updateScript']['maqlDdlChunks'];
								}
								if (!$updateScript['updateScript']['preserveData'] && $updateScript['updateScript']['cascadeDrops']) {
									$cascadeDrops = $updateScript['updateScript']['maqlDdlChunks'];
								}
							}

							$description = array();
							foreach ($taskResponse['projectModelDiff']['updateOperations'] as $o) {
								$description[] = vsprintf($o['updateOperation']['description'], $o['updateOperation']['parameters']);
							}

							return array(
								'moreDestructiveMaql' => $cascadeDrops,
								'lessDestructiveMaql' => $noCascadeDrops,
								'description' => $description
							);
						} else {
							$this->logAlert('updateProjectModel() has bad response', array(
								'uri' => $uri,
								'result' => $result
							));
							throw new RestApiException('Update Project Model task could not be finished');
						}
					}

					$try++;
				} while (true);

			} else {
				$this->logAlert('updateProjectModel() has bad response', array(
					'uri' => $uri,
					'result' => $result
				));
				throw new RestApiException('Update Project Model task could not be started');
			}

			return false;
		} catch (RestApiException $e) {
			$details = $e->getDetails();
			if (isset($details['details']['error']['validationErrors'])) {
				$errors = array();
				foreach ($details['details']['error']['validationErrors'] as $err) {
					$errors[] = vsprintf($err['validationError']['message'], $err['validationError']['parameters']);
				}
				if (count($errors)) {
					$e->setDetails($errors);
				}
			}
			throw $e;
		}
	}

	public function getProjectModel($pid)
	{
		$uri = sprintf('/gdc/projects/%s/model/view', $pid);
		$result = $this->jsonRequest($uri, 'GET');

		if (isset($result['asyncTask']['link']['poll'])) {

			$try = 1;
			do {
				sleep(10 * $try);
				$taskResponse = $this->jsonRequest($result['asyncTask']['link']['poll']);

				if (!isset($taskResponse['asyncTask']['link']['poll'])) {
					if (isset($taskResponse['projectModelView']['model'])) {
						return $taskResponse['projectModelView']['model'];
					} else {
						$this->logAlert('getProjectModel() has bad response', array(
							'uri' => $uri,
							'result' => $result
						));
						throw new RestApiException('Get Project Model task could not be finished');
					}
				}

				$try++;
			} while (true);

		} else {
			$this->logAlert('getProjectModel() has bad response', array(
				'uri' => $uri,
				'result' => $result
			));
			throw new RestApiException('Get Project Model task could not be started');
		}

		return false;
	}



	/**
	 * Drop dataset with it's folders
	 */
	public function dropDataSet($pid, $dataSetName)
	{
		$dataSetId = Model::getId($dataSetName);
		$model = $this->getProjectModel($pid);
		if (isset($model['projectModel']['datasets'])) foreach ($model['projectModel']['datasets'] as $i => $dataSet) {
			if ($dataSet['dataset']['title'] == $dataSetName) {
				unset($model['projectModel']['datasets'][$i]);
				break;
			}
		}
		$model['projectModel']['datasets'] = array_values($model['projectModel']['datasets']);

		$update = $this->generateUpdateProjectMaql($pid, $model);

		if (count($update['moreDestructiveMaql'])) {
			foreach($update['moreDestructiveMaql'] as $m) {
				$this->executeMaql($pid, $m);
			}
			$this->executeMaql($pid, sprintf('DROP IF EXISTS {dim.%s};', $dataSetId));
			$this->executeMaql($pid, sprintf('DROP IF EXISTS {ffld.%s};', $dataSetId));

			return $update['description'];
		}
		return false;
	}


	public function executeReport($uri)
	{
		$this->request('/gdc/xtab2/executor3', 'POST', array(
			'report_req' => array(
				'report' => $uri
			)
		));
	}

	/**
	 * @param string $pid
	 * @param string $uri - Report Definition URI
	 * @return \Guzzle\Http\EntityBodyInterface|\Guzzle\Http\Message\Response|null|string
	 */
	public function executeReportRaw($pid, $uri)
	{
		return $this->request(sprintf('/gdc/app/projects/%s/execute/raw/', $pid), 'POST', array(
			"report_req" => array(
				"reportDefinition" => $uri
			)
		))->json();
	}

	public function createDateDimension($pid, $name, $includeTime=false, $template=null)
	{
		$identifier = Model::getId($name);
		$dataSets = $this->getDataSets($pid);

		$maql = '';
		if (!in_array(Model::getDateDimensionId($name, $template), array_keys($dataSets))) {
			$template = $template? strtoupper($template) : 'GOODDATA';
			$maql .= sprintf('INCLUDE TEMPLATE "URN:%s:DATE" MODIFY (IDENTIFIER "%s", TITLE "%s");', $template, $identifier, $name);
		}

		if ($includeTime && !in_array(Model::getTimeDimensionId($name), array_keys($dataSets))) {
			$maql .= 'CREATE DATASET {dataset.time.%ID%} VISUAL(TITLE "Time (%NAME%)");';
			$maql .= 'CREATE FOLDER {dim.time.%ID%} VISUAL(TITLE "Time dimension (%NAME%)") TYPE ATTRIBUTE;';
			$maql .= 'CREATE FOLDER {ffld.time.%ID%} VISUAL(TITLE "Time dimension (%NAME%)") TYPE FACT;';

			$maql .= 'CREATE ATTRIBUTE {attr.time.second.of.day.%ID%} VISUAL(TITLE "Time (%NAME%)",'
				. ' FOLDER {dim.time.%ID%}) AS KEYS {d_time_second_of_day_%ID%.id} FULLSET WITH LABELS'
				. ' {label.time.%ID%} VISUAL(TITLE "Time (hh:mm:ss)") AS {d_time_second_of_day_%ID%.nm},'
				. ' {label.time.twelve.%ID%} VISUAL(TITLE "Time (HH:mm:ss)") AS {d_time_second_of_day_%ID%.nm_12},'
				. ' {label.time.second.of.day.%ID%} VISUAL(TITLE "Second of Day") AS {d_time_second_of_day_%ID%.nm_sec};';
			$maql .= 'ALTER ATTRIBUTE {attr.time.second.of.day.%ID%} ORDER BY {label.time.%ID%} ASC;';
			$maql .= 'ALTER DATASET {dataset.time.%ID%} ADD {attr.time.second.of.day.%ID%};';

			$maql .= 'CREATE ATTRIBUTE {attr.time.second.%ID%} VISUAL(TITLE "Second (%NAME%)",'
				. ' FOLDER {dim.time.%ID%}) AS KEYS {d_time_second_%ID%.id} FULLSET,'
				. ' {d_time_second_of_day_%ID%.second_id} WITH LABELS'
				. ' {label.time.second.%ID%} VISUAL(TITLE "Second") AS {d_time_second_%ID%.nm};';
			$maql .= 'ALTER DATASET {dataset.time.%ID%} ADD {attr.time.second.%ID%};';

			$maql .= 'CREATE ATTRIBUTE {attr.time.minute.of.day.%ID%} VISUAL(TITLE "Minute of Day (%NAME%)",'
				. ' FOLDER {dim.time.%ID%}) AS KEYS {d_time_minute_of_day_%ID%.id} FULLSET,'
				. ' {d_time_second_of_day_%ID%.minute_id} WITH LABELS'
				. ' {label.time.minute.of.day.%ID%} VISUAL(TITLE "Minute of Day") AS {d_time_minute_of_day_%ID%.nm};';
			$maql .= 'ALTER DATASET {dataset.time.%ID%} ADD {attr.time.minute.of.day.%ID%};';

			$maql .= 'CREATE ATTRIBUTE {attr.time.minute.%ID%} VISUAL(TITLE "Minute (%NAME%)",'
				. ' FOLDER {dim.time.%ID%}) AS KEYS {d_time_minute_%ID%.id} FULLSET,'
				. ' {d_time_minute_of_day_%ID%.minute_id} WITH LABELS'
				. ' {label.time.minute.%ID%} VISUAL(TITLE "Minute") AS {d_time_minute_%ID%.nm};';
			$maql .= 'ALTER DATASET {dataset.time.%ID%} ADD {attr.time.minute.%ID%};';

			$maql .= 'CREATE ATTRIBUTE {attr.time.hour.of.day.%ID%} VISUAL(TITLE "Hour (%NAME%)",'
				. ' FOLDER {dim.time.%ID%}) AS KEYS {d_time_hour_of_day_%ID%.id} FULLSET,'
				. ' {d_time_minute_of_day_%ID%.hour_id} WITH LABELS'
				. ' {label.time.hour.of.day.%ID%} VISUAL(TITLE "Hour (0-23)") AS {d_time_hour_of_day_%ID%.nm},'
				. ' {label.time.hour.of.day.twelve.%ID%} VISUAL(TITLE "Hour (1-12)") AS {d_time_hour_of_day_%ID%.nm_12};';
			$maql .= 'ALTER ATTRIBUTE {attr.time.hour.of.day.%ID%} ORDER BY {label.time.hour.of.day.%ID%} ASC;';
			$maql .= 'ALTER DATASET {dataset.time.%ID%} ADD {attr.time.hour.of.day.%ID%};';

			$maql .= 'CREATE ATTRIBUTE {attr.time.ampm.%ID%} VISUAL(TITLE "AM/PM (%NAME%)", '
				. 'FOLDER {dim.time.%ID%}) AS KEYS {d_time_ampm_%ID%.id} FULLSET,'
				. ' {d_time_hour_of_day_%ID%.ampm_id} WITH LABELS'
				. ' {label.time.ampm.%ID%} VISUAL(TITLE "AM/PM") AS {d_time_ampm_%ID%.nm};';
			$maql .= 'ALTER DATASET {dataset.time.%ID%} ADD {attr.time.ampm.%ID%};';

			$maql .= 'SYNCHRONIZE {dataset.time.%ID%};';

			$maql = str_replace('%ID%', $identifier, $maql);
			$maql = str_replace('%NAME%', $name, $maql);
		}

		if ($maql) {
			$this->executeMaql($pid, $maql);
		}
	}




	/**
	 * Execute Maql asynchronously and wait for result
	 */
	public function executeMaql($pid, $maql)
	{
		$uri = sprintf('/gdc/md/%s/ldm/manage2', $pid);
		$params = array(
			'manage' => array(
				'maql' => $maql
			)
		);
		$result = $this->jsonRequest($uri, 'POST', $params);

		$pollLink = null;
		if (isset($result['entries']) && count($result['entries'])) {
			foreach ($result['entries'] as $entry) {
				if (isset($entry['category']) && isset($entry['link']) && $entry['category'] == 'tasks-status') {
					$pollLink = $entry['link'];
					break;
				}
			}
		}

		if (empty($pollLink)) {
			$this->logAlert('executeMaqlAsync() result is missing status url in entries.link', array(
				'uri' => $uri,
				'maql' => $maql,
				'result' => $result
			));
			throw new RestApiException('Error in result of /ldm/manage2 task');
		}

		$try = 1;
		do {
			sleep(10 * $try);
			$taskResponse = $this->jsonRequest($pollLink);

			if (!isset($taskResponse['wTaskStatus']['status'])) {
				$this->logAlert('executeMaql() has bad response', array(
					'uri' => $result['pullTask']['uri'],
					'result' => $taskResponse
				));
				throw new RestApiException('Task /ldm/manage2 could not be checked');
			}

			$try++;
		} while (in_array($taskResponse['wTaskStatus']['status'], array('PREPARED', 'RUNNING')));

		if ($taskResponse['wTaskStatus']['status'] == 'ERROR') {
			$messages = isset($taskResponse['wTaskStatus']['messages']) ? $taskResponse['wTaskStatus']['messages'] : null;
			throw new RestApiException('Task /ldm/manage2 finished with error', $messages);
		}

		return $taskResponse;
	}


	/**
	 * Creates new Mandatory User Filter
	 *
	 */
	public function createFilter($name, $attribute, $operator, $value, $pid)
	{
		$gdAttribute = $this->getAttributeById($pid, $attribute);

		if (is_array($value)) {
			$elementArr = array();
			foreach ($value as $v) {
				$elementArr[] = '[' . $this->getElementUriByTitle($gdAttribute['content']['displayForms'][0]['links']['elements'], $v) . ']';
			}

			$elementsUri = implode(',', $elementArr);
			$expression = "[" . $gdAttribute['meta']['uri'] . "]" . $operator . "(" . $elementsUri . ")";
		} else {
			$elementUri = $this->getElementUriByTitle($gdAttribute['content']['displayForms'][0]['links']['elements'], $value);
			$expression = "[" . $gdAttribute['meta']['uri'] . "]" . $operator . "[" . $elementUri . "]";
		}

		$filterUri = sprintf('/gdc/md/%s/obj', $pid);
		$params = array(
			'userFilter' => array(
				'content' => array(
					'expression' => $expression
				),
				'meta' => array(
					'category'  => 'userFilter',
					'title'     => $name
				)
			)
		);
		$result = $this->jsonRequest($filterUri, 'POST', $params);

		if (isset($result['uri'])) {
			return $result['uri'];
		} else {
			$this->logAlert('createFilters() has bad response', array(
				'uri' => $filterUri,
				'params' => $params,
				'result' => $result
			));
			throw new RestApiException('Error in attempt to create filter');
		}
	}

	public function assignFiltersToUser(array $filters, $userId, $pid)
	{
		$uri = sprintf('/gdc/md/%s/userfilters', $pid);

		$params = array(
			'userFilters' => array(
				'items' => array(
					array(
						"user" => "/gdc/account/profile/" . $userId,
						"userFilters" => $filters
					)
				)
			)
		);
		$result = $this->jsonRequest($uri, 'POST', $params);

		if (isset($result['userFiltersUpdateResult']['successful']) && count($result['userFiltersUpdateResult']['successful'])) {
			// SUCCESS
		} else {
			throw new RestApiException('Error in attempt to assign filters to user');
		}
	}

	public function deleteFilter($filterUri)
	{
		$this->jsonRequest($filterUri, 'DELETE');
	}

	public function getFilters($pid)
	{
		$uri = sprintf('/gdc/md/%s/query/userfilters', $pid);
		$result = $this->jsonRequest($uri);

		if (isset($result['query']['entries'])) {
			return $result['query']['entries'];
		} else {
			$this->logAlert('getFilters() has bad response', array(
				'uri' => $uri,
				'result' => $result
			));
			throw new RestApiException('Filters in project could not be fetched');
		}
	}

	public function getAttributes($pid)
	{
		$uri = sprintf('/gdc/md/%s/query/attributes', $pid);
		$result = $this->jsonRequest($uri);

		if (isset($result['query']['entries'])) {
			return $result['query']['entries'];
		} else {
			$this->logAlert('getAttributes() has bad response', array(
				'uri' => $uri,
				'result' => $result
			));
			throw new RestApiException('Attributes in project could not be fetched');
		}
	}

	public function getAttributeById($pid, $id)
	{
		return $this->getAttribute($pid, 'identifier', $id);
	}

	public function getAttributeByTitle($pid, $title)
	{
		return $this->getAttribute($pid, 'title', $title);
	}

	public function getAttribute($pid, $field=null, $search=null)
	{
		$attributes = $this->getAttributes($pid);
		$attrUri = null;

		foreach ($attributes as $attr) {
			if (isset($attr[$field]) && $attr[$field] == $search) {
				$attrUri = $attr['link'];
				break;
			}
		}

		if (null == $attrUri) {
			throw new JobProcessException(sprintf('Attribute with %s = %s not found in project', $field, $search));
		} else {
			$result = $this->jsonRequest($attrUri);
			if (isset($result['attribute'])) {
				return $result['attribute'];
			} else {
				$this->logAlert('getAttribute() has bad response', array(
					'uri' => $attrUri,
					'result' => $result
				));
				throw new RestApiException(sprintf("Attribute '%s' with uri '%s' could not be fetched", $search, $attrUri));
			}
		}
	}

	public function getElements($uri)
	{
		$result = $this->jsonRequest($uri);
		if (isset($result['attributeElements']['elements'])) {
			return $result['attributeElements']['elements'];
		} else {
			$this->logAlert('getElements() has bad response', array(
				'uri' => $uri,
				'result' => $result
			));
			throw new RestApiException('Elements could not be fetched');
		}
	}

	public function getElementUriByTitle($uri, $title)
	{
		$elementUri = null;
		foreach ($this->getElements($uri) as $e) {
			if ($e['title'] == $title) {
				$elementUri = $e['uri'];
				break;
			}
		}

		if (null == $elementUri) {
			throw new JobProcessException('Element ' . $title . ' not found');
		} else {
			return $elementUri;
		}
	}

	public function getWebDavUrl()
	{
		$gdc = $this->get('/gdc');
		if (isset($gdc['about']['links'])) foreach ($gdc['about']['links'] as $link) {
			if ($link['category'] == 'uploads') {
				return $link['link'];
			}
		}
		return false;
	}

	public function post($url, $payload = null)
	{
		if (null != $payload) {
			// trick to get rid of "labels": {} problem
			$this->fixLabelsRec($payload);
		}

		$response = $this->jsonRequest($url, 'POST', $payload);

		return $response;
	}

	protected function fixLabelsRec(&$array)
	{
		foreach ($array as $key => &$item) {
			if ($key == 'labels') {
				if ($key == 'labels' && empty($item)) {
					$item = new stdClass();
				}
			}

			if (is_array($item)) {
				$this->fixLabelsRec($item);
			}
		}
	}



	public function optimizeSliHash($pid, $manifests=array())
	{
		if (count($manifests)) {
			$result = $this->post(sprintf('/gdc/md/%s/etl/mode', $pid), array(
				'etlMode' => array(
					'mode' => 'SLI',
					'lookup' => 'recreate',
					'sli' => $manifests
				)
			));
			if (empty($result['uri'])) {
				$this->logAlert('optimizeSliHash task has bad response', array(
					'uri' => $result['pullTask']['uri'],
					'result' => $result
				));
				throw new RestApiException('Bad response from optimizeSliHash call' . json_encode($result));
			}
			$pollLink = $result['uri'];

			$try = 0;
			do {
				sleep(10 * $try);
				$taskResponse = $this->jsonRequest($pollLink);

				if (!isset($taskResponse['wTaskStatus']['status'])) {
					$this->logAlert('optimizeSliHash task has bad response', array(
						'uri' => $result['pullTask']['uri'],
						'result' => $taskResponse
					));
					throw new RestApiException('Task optimizeSliHash could not be checked');
				}

				$try++;
			} while (in_array($taskResponse['wTaskStatus']['status'], array('PREPARED', 'RUNNING')));
		}
	}



	/**
	 * Poll task uri and wait for its finish
	 * @param $uri
	 * @throws RestApiException
	 */
	public function waitForTask($uri)
	{
		$repeat = true;
		$i = 0;
		do {
			sleep(self::WAIT_INTERVAL * ($i + 1));

			$result = $this->jsonRequest($uri);
			if (isset($result['taskState']['status'])) {
				if (in_array($result['taskState']['status'], array('OK', 'ERROR', 'WARNING'))) {
					$repeat = false;
				}
			} else {
				$this->logAlert('waitForTask() has bad response', array(
					'uri' => $uri,
					'result' => $result
				));
				throw new RestApiException('Bad response');
			}

			$i++;
		} while ($repeat);

		if ($result['taskState']['status'] != 'OK') {
			$this->logAlert('waitForTask() has bad response', array(
				'uri' => $uri,
				'result' => $result
			));
			throw new RestApiException('Bad response');
		}
	}


	/**
	 * Common request expecting json result
	 */
	protected function jsonRequest($uri, $method = 'GET', $params = array(), $headers = array(), $logCall = true)
	{
		return $this->request($uri, $method, $params, $headers, $logCall)->json();
	}

	public function getStream($uri)
	{
		if (!$this->authTt) {
			$this->refreshToken();
		}

		$request = $this->client->get($uri);

		if ($this->authSst) {
			$request->addCookie('GDCAuthSST', $this->authSst);
		}
		if ($this->authTt) {
			$request->addCookie('GDCAuthTT', $this->authTt);
		}

		$streamFactory = new PhpStreamRequestFactory();
		$stream = $streamFactory->fromRequest($request);

		return $stream;
	}

	public function getToFile($uri, $filename)
	{
		if (!$this->authTt) {
			$this->refreshToken();
		}
		$startTime = time();

		$request = $this->client->get($this->apiUrl . $uri, array(
			'accept' => 'text/csv',
			'accept-charset' => 'utf-8'
		));

		$retriesCount = 1;
		do {
			$isMaintenance = false;

			if ($this->authSst) {
				$request->addCookie('GDCAuthSST', $this->authSst);
			}
			if ($this->authTt) {
				$request->addCookie('GDCAuthTT', $this->authTt);
			}

			$request->setResponseBody($filename);

			try {
				$response = $request->send();

				$this->logCall($uri, 'GET', array("filename" => $filename), '["-- omitted --"]', time() - $startTime, $response->getStatusCode());

				if ($response->getStatusCode() == 200) {
					return $filename;
				}
			} catch (\Exception $e) {

				if ($e instanceof ClientErrorResponseException) {
					$response = $request->getResponse();
					$this->logCall($uri, 'GET', array("filename" => $filename), $response->getBody(true), time() - $startTime, $response->getStatusCode());

					if ($request->getResponse()->getStatusCode() == 201) {

						// file not ready yet do nothing

					} elseif ($request->getResponse()->getStatusCode() == 401) {

						// TT token expired
						$this->refreshToken();

					} else {
						throw new RestApiException("Error occurred while downloading file. Status code ". $response->getStatusCode(), 500, $e);
					}
				} elseif ($e instanceof ServerErrorResponseException) {
					if ($request->getResponse()->getStatusCode() == 503) {
						// GD maintenance
						$isMaintenance = true;
					}
				} elseif ($e instanceof CurlException) {
					// Just retry according to backoff algorithm
				} else {
					throw $e;
				}
			}

			if ($isMaintenance) {
				sleep(rand(60, 600));
			} else {
				sleep(self::BACKOFF_INTERVAL * ($retriesCount + 1));
				$retriesCount++;
			}

		} while ($isMaintenance || $retriesCount <= self::RETRIES_COUNT);

		return false;
	}


	/**
	 * @return \Guzzle\Http\Message\Response
	 */
	private function request($uri, $method = 'GET', $params = array(), $headers = array(), $logCall = true, $refreshToken = true)
	{
		if ($this->eventLogger) {
			$this->eventLogger->log($this->jobId, $this->runId, sprintf('Rest API call %s %s started', $method, $uri));
		}

		if ($refreshToken && !$this->authTt) {
			$this->refreshToken();
		}

		$startTime = time();
		$jsonParams = is_array($params) ? json_encode($params) : $params;

		$request = null;
		$response = null;
		$exception = null;
		$retriesCount = 1;
		do {
			$isMaintenance = false;
			switch ($method) {
				case 'GET':
					$request = $this->client->get($uri, $headers);
					break;
				case 'POST':
					$request = $this->client->post($uri, $headers, $jsonParams);
					$request->getCurlOptions()->set(CurlHandle::BODY_AS_STRING, true);
					break;
				case 'PUT':
					$request = $this->client->put($uri, $headers, $jsonParams);
					$request->getCurlOptions()->set(CurlHandle::BODY_AS_STRING, true);
					break;
				case 'DELETE':
					$request = $this->client->delete($uri, $headers);
					break;
				default:
					throw new RestApiException('Unsupported request method "' . $method . '"');
			}
			if ($this->authSst) {
				$request->addCookie('GDCAuthSST', $this->authSst);
			}
			if ($this->authTt) {
				$request->addCookie('GDCAuthTT', $this->authTt);
			}

			$exception = null;
			try {
				$response = $request->send();

				$duration = time() - $startTime;
				if ($logCall) $this->logCall($uri, $method, $params, $response->getBody(true), $duration, $request->getResponse()->getStatusCode());

				$this->logUsage($uri, $method, $params, $headers, $request, $duration);
				if ($response->isSuccessful()) {

					if ($this->eventLogger) {
						$this->eventLogger->log($this->jobId, $this->runId, sprintf('Rest API call %s %s finished', $method, $uri), null, null, $startTime);
					}
					return $response;
				}

			} catch (\Exception $e) {
				$exception = $e;
				$duration = time() - $startTime;

				$responseObject = $request->getResponse();
				if ($responseObject) {
					$response = $request->getResponse()->getBody(true);
				}

				if ($logCall) $this->logCall($uri, $method, $params, $response, $duration, $responseObject? $responseObject->getStatusCode() : null);
				$this->logUsage($uri, $method, $params, $headers, $request, $duration);

				$responseJson = json_decode($response, true);
				if ($e instanceof ClientErrorResponseException) {
					if ($responseObject && $responseObject->getStatusCode() == 401) {
						if (isset($responseJson['message']) && $responseJson['message'] == 'Login needs security code verification due to failed login attempts.') {
							// Bad password
							throw new \Exception('Login "' . $this->username . '" refused due to failed login attempts');
						} else {
							// TT token expired
							//$this->refreshToken();
							$this->login($this->username, $this->password);
						}
					} elseif ($responseObject && $responseObject->getStatusCode() == 403) {
						throw new RestApiException('GoodData user ' . $this->username . ' does not have access to the project, it has been probably disabled.', $response, $request->getResponse()->getStatusCode());
					} elseif ($responseObject && $responseObject->getStatusCode() == 410) {
						throw new RestApiException('Rest API url ' . $uri . ' is not reachable, GoodData project has been probably deleted.', $response, $request->getResponse()->getStatusCode());
					} else {
						if ($responseJson !== false) {
							// Include parameters directly to error message
							if (isset($responseJson['error']) && isset($responseJson['error']['parameters']) && isset($responseJson['error']['message'])) {
								$responseJson['error']['message'] = vsprintf($responseJson['error']['message'], $responseJson['error']['parameters']);
								unset($responseJson['error']['parameters']);
							}
							$response = json_encode($responseJson);
						}
						throw new RestApiException('API error ' . $request->getResponse()->getStatusCode(), $response, $request->getResponse()->getStatusCode());
					}
				} elseif ($e instanceof ServerErrorResponseException) {
					if ($responseObject && $responseObject->getStatusCode() == 503) {
						// GD maintenance
						$isMaintenance = true;
					}
				} elseif ($e instanceof CurlException) {
					// Just retry according to backoff algorithm
				} else {
					throw $e;
				}

			}

			if ($isMaintenance) {
				sleep(rand(60, 600));
			} else {
				sleep(self::BACKOFF_INTERVAL * ($retriesCount + 1));
				$retriesCount++;
				$this->refreshToken();
			}

		} while ($isMaintenance || $retriesCount <= self::RETRIES_COUNT);

		/** @var $response \Guzzle\Http\Message\Response */
		$statusCode = $request ? $request->getResponse()->getStatusCode() : null;
		$this->logAlert('GoodData API error', array(
			'uri' => $uri,
			'method' => $method,
			'params' => $jsonParams,
			'headers' => $headers,
			'response' => $response,
			'status' => $statusCode,
			'exception' => $exception
		));
		throw new RestApiException('GoodData API error ' . $statusCode, $response);
	}

	protected function logUsage($uri, $method, $params, $headers, RequestInterface $request, $duration)
	{
		if (!$this->usageLogger) {
			return;
		}

		$response = $request->getResponse();
		$this->usageLogger->debug($method . ' ' . $uri, array(
			'jobId' => $this->jobId,
			'request' => array(
				'params' => $params,
				'headers' => $headers,
				'response' => array(
					'status' => $response? $response->getStatusCode() : null,
					'body' => $response? $response->getBody(true) : null
				)
			),
			'duration' => $duration
		));
	}

	/**
	 * @param $username
	 * @param $password
	 * @throws RestApiException
	 */
	public function login($username, $password)
	{
		$this->username = $username;
		$this->password = $password;
		$this->clearFromLog[] = $password;

		if (!$this->username || !$this->password) {
			throw new RestApiException('Rest API login failed, missing username or password');
		}

		try {
			$response = $this->request('/gdc/account/login', 'POST', array(
				'postUserLogin' => array(
					'login' => $username,
					'password' => $password,
					'remember' => 0
				)
			), array(), true, false);
		} catch (RestApiException $e) {
			throw new RestApiException('Rest API Login failed', $e->getMessage());
		}

		$this->authSst = $this->findCookie($response, 'GDCAuthSST');
		if (!$this->authSst) {
			$this->logAlert('Invalid login - GDCAuthSST not found', array(
				'response' => $response->getBody(true),
				'status' => $response->getStatusCode()
			));
			throw new RestApiException('Rest API login failed');
		}

		$this->refreshToken();

	}

	public function refreshToken()
	{
		try {
			$response = $this->request('/gdc/account/token', 'GET', array(), array(), true, false);
		} catch (RestApiException $e) {
			throw new RestApiException('Refresh token failed', $e->getMessage());
		}

		$this->authTt = $this->findCookie($response, 'GDCAuthTT');
		if (!$this->authTt) {
			$this->logAlert('Invalid login - GDCAuthTT not found', array(
				'response' => $response->getBody(true),
				'status' => $response->getStatusCode()
			));
			throw new RestApiException('Rest refresh token failed');
		}
	}

	/**
	 * Utility function: Retrieves specified cookie from supplied response headers
	 * NB: Very basic parsing - ignores path, domain, expiry
	 *
	 * @param \Guzzle\Http\Message\Response $response
	 * @param $name
	 * @return string or null if specified cookie not found
	 * @author Jakub Nesetril
	 */
	protected function findCookie($response, $name)
	{
		$header = $response->getHeader('Set-cookie');
		if ($header instanceof Header) {
			$cookies = $header->toArray();
			$cookie = array_filter($cookies, function($cookie) use($name) {
				return strpos($cookie, $name) === 0;
			});
			$cookie = reset($cookie);
			if (empty($cookie)) {
				return false;
			}

			$cookie = explode('; ', $cookie);
			$cookie = reset($cookie);
			return substr($cookie, strpos($cookie, '=') + 1);
		} else {
			return false;
		}
	}


	protected function logAlert($message, $context)
	{
		$this->logger->alert($message, array_merge($context, array('jobId' => $this->jobId)));
	}


	protected function logCall($uri, $method, $params, $response, $duration, $statusCode)
	{
		$decodedResponse = json_decode($response, true);
		if (!$decodedResponse) {
			$decodedResponse = array($response);
		}

		$clearFromLog = $this->clearFromLog;
		$sanitize = function(&$value) use($clearFromLog) {
			if ($value && !is_object($value) && in_array((string)$value, $clearFromLog)) {
				$value = '***';
			}
		};
		array_walk_recursive($params, $sanitize);
		array_walk_recursive($decodedResponse, $sanitize);

		$this->logFile->fwrite(json_encode(array(
			'time' => date('c'),
			'duration' => $duration,
			'uri' => $this->client->getBaseUrl() . $uri,
			'method' => $method,
			'params' => $params,
			'status' => $statusCode,
			'response' => $decodedResponse
		), JSON_PRETTY_PRINT) . ',');
	}

	public function initLog()
	{
		$this->logFile = $this->tempService->createTmpFile('.json')->openFile('a');
		$this->logFile->fwrite('[');
	}

	public function getLogPath()
	{
		$this->logFile->fwrite('null]');
		return $this->logFile->getRealPath();
	}

}
