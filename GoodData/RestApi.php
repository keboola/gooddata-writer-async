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

	public static $userRoles = array(
		'admin' => 'adminRole',
		'editor' => 'editorRole',
		'readOnly' => 'readOnlyUserRole',
		'dashboardOnly' => 'dashboardOnlyRole'
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

	protected $authSst;
	protected $authTt;

	private $jobId;
	private $clearFromLog;

	private $apiUrl;
	private $username;
	private $password;



	public function __construct(Model $model, Logger $logger, Logger $usageLogger, TempServiceFactory $tempServiceFactory)
	{
		if (!defined('JSON_PRETTY_PRINT')) {
			// fallback for PHP <= 5.3
			define('JSON_PRETTY_PRINT', 0);
		}


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

		$this->tempService = $tempServiceFactory->get('gooddata_writer');
		$this->clearFromLog = array();

		$this->initLog();
	}

	public function setJobId($jobId)
	{
		$this->jobId = $jobId;
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
	 * @param $name
	 * @param $authToken
	 * @throws RestApiException
	 * @return string
	 */
	public function createProject($name, $authToken)
	{
		$this->clearFromLog[] = $authToken;

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
				'login' => $login,
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
				return $userId;
			} else {
				throw $e;
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
			if (!empty($user['accountSetting']['login']) && $user['accountSetting']['login'] == $email) {
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
		$result = $this->jsonRequest(sprintf('/gdc/projects/%s/users', $pid));
		return isset($result['users']) ? $result['users'] : array();
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
			if (!empty($user['user']['content']['email']) && $user['user']['content']['email'] == $email) {
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
	public function addUserToProject($userId, $pid, $role = 'adminRole')
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
			$this->logAlert('addUserToProject() has not added user to project', array(
				'uri' => $uri,
				'params' => $params,
				'result' => $result
			));
			throw new RestApiException('Error in addition to project');
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
	public function inviteUserToProject($email, $pid, $role = 'adminRole')
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
			// SUCCESS
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

	/**
	 * Cancel User Invitation to Project
	 *
	 * @param $email
	 * @param $pid
	 * @throws \Exception|\Guzzle\Http\Exception\ClientErrorResponseException
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

				if ($invitationEmail != $email)
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
	public function loadData($pid, $dirName, $dataSetName)
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
				$uploadMessage = $this->getUploadMessage($pid, $dataSetName);
				throw new RestApiException($uploadMessage ? $uploadMessage : 'ETL task finished with error');
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
		$model = $this->getProjectModel($pid);
		foreach ($model['projectModel']['datasets'] as &$dataSet) {
			if ($dataSet['dataset']['title'] == $definition['name']) {
				$dataSet['dataset'] = $this->model->getLDM($definition);
				break;
			}
		}
		$resultMaql = $this->generateUpdateProjectMaql($pid, $model);
		if ($resultMaql) {
			$this->executeMaql($pid, $resultMaql);
		}
	}

	public function generateUpdateProjectMaql($pid, $model)
	{
		$uri = sprintf('/gdc/projects/%s/model/diff', $pid);
		$result = $this->jsonRequest($uri, 'POST', array('diffRequest' => array('targetModel' => $model)));

		if (isset($result['asyncTask']['link']['poll'])) {

			$try = 1;
			do {
				sleep(10 * $try);
				$taskResponse = $this->jsonRequest($result['asyncTask']['link']['poll']);

				if (!isset($taskResponse['asyncTask']['link']['poll'])) {
					if (isset($taskResponse['projectModelDiff']['updateScripts'])) {
						$preserveDataMaql = null;
						$noPreserveDataMaql = null;
						foreach($taskResponse['projectModelDiff']['updateScripts'] as $updateScript) {
							if ($updateScript['updateScript']['preserveData'] && !$updateScript['updateScript']['cascadeDrops']) {
								$preserveDataMaql = $updateScript['updateScript']['maqlDdl'];
							}
							if (!$updateScript['updateScript']['preserveData'] && !$updateScript['updateScript']['cascadeDrops']) {
								$noPreserveDataMaql = $updateScript['updateScript']['maqlDdl'];
							}
						}
						return $preserveDataMaql ? $preserveDataMaql : $noPreserveDataMaql;
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
	 * Drop dataset
	 * @param $pid
	 * @param $dataset
	 * @return string
	 */
	public function dropDataset($pid, $dataset)
	{
		//@TODO Find dataset identificator
		$maql  = sprintf('DROP IF EXISTS {dim.%s};', $dataset);
		$maql .= sprintf('DROP IF EXISTS {ffld.%s};', $dataset);
		$maql .= sprintf('DROP ALL IN IF EXISTS {dataset.%s};', $dataset);

		return $this->executeMaql($pid, $maql);
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

	public function getUploadMessage($pid, $datasetName)
	{
		$datasets = $this->get(sprintf('/gdc/md/%s/data/sets', $pid));
		foreach ($datasets['dataSetsInfo']['sets'] as $dataset) {
			if ($dataset['meta']['identifier'] == 'dataset.' . $datasetName) {
				return $dataset['lastUpload']['dataUploadShort']['msg'];
			}
		}

		return null;
	}

	public function createDateDimension($pid, $name, $includeTime = false)
	{
		$identifier = Model::getId($name);
		$dataSets = $this->getDataSets($pid);

		$maql = '';
		if (!in_array(Model::getDateDimensionId($name), array_keys($dataSets))) {
			$maql .= sprintf('INCLUDE TEMPLATE "URN:GOODDATA:DATE" MODIFY (IDENTIFIER "%s", TITLE "%s");', $identifier, $name);
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
	 * @param $name
	 * @param $attribute
	 * @param $element
	 * @param $operator
	 * @param $pid
	 * @throws RestApiException
	 * @return mixed
	 */
	public function createFilter($name, $attribute, $element, $operator, $pid)
	{
		$gdAttribute = $this->getAttributeById($pid, $attribute);

		if (is_array($element)) {
			$elementArr = array();
			foreach ($element as $e) {
				$elementArr[] = '[' . $this->getElementUriByTitle(
						$gdAttribute['content']['displayForms'][0]['links']['elements'],
						$e
					) . ']';
			}

			$elementsUri = implode(',', $elementArr);
			$expression = "[" . $gdAttribute['meta']['uri'] . "]" . $operator . "(" . $elementsUri . ")";
		} else {
			$elementUri = $this->getElementUriByTitle(
				$gdAttribute['content']['displayForms'][0]['links']['elements'],
				$element
			);

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
			$this->logAlert('assignFiltersToUser() has bad response', array(
				'uri' => $uri,
				'params' => $params,
				'result' => $result
			));
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
		$attributes = $this->getAttributes($pid);

		foreach ($attributes as $attr) {
			$object = $this->jsonRequest($attr['link']);
			if (isset($object['attribute']['meta']['identifier'])) {
				if ($object['attribute']['meta']['identifier'] == $id) {
					return $object['attribute'];
				}
			}
		}

		$this->logAlert('getAttributeById() attribute not found', array(
			'pid' => $pid,
			'attribute' => $id,
			'attributesFound' => $attributes
		));
		throw new RestApiException('Attribute ' . $id . ' not found in project.');
	}

	public function getAttributeByTitle($pid, $title)
	{
		$attributes = $this->getAttributes($pid);
		$attrUri = null;

		foreach ($attributes as $attr) {
			if ($attr['title'] == $title) {
				$attrUri = $attr['link'];
				break;
			}
		}

		if (null == $attrUri) {
			$this->logAlert('getAttributeByTitle() attribute not found', array(
				'pid' => $pid,
				'attribute' => $title
			));
			throw new RestApiException('Attribute ' . $title . ' not found in project');
		} else {
			$result = $this->jsonRequest($attrUri);
			if (isset($result['attribute'])) {
				return $result['attribute'];
			} else {
				$this->logAlert('getAttributeByTitle() bad response', array(
					'uri' => $attrUri,
					'result' => $result
				));
				throw new RestApiException('Attribute ' . $attrUri . ' could not be fetched');
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
			$this->logAlert('getElementsByTitle() element not found', array(
				'uri' => $uri,
				'element' => $title
			));
			throw new RestApiException('Element ' . $title . ' not found');
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
		$startTime = microtime();

		$request = $this->client->get($this->apiUrl . $uri, array(
			'accept' => 'text/csv',
			'accept-charset' => 'utf-8'
		));

		for ($i = 0; $i < self::RETRIES_COUNT; $i++) {
			if ($this->authSst) {
				$request->addCookie('GDCAuthSST', $this->authSst);
			}
			if ($this->authTt) {
				$request->addCookie('GDCAuthTT', $this->authTt);
			}

			$request->setResponseBody($filename);

			try {
				$response = $request->send();

				$this->logCall($uri, 'GET', array("filename" => $filename), $response->getBody(true), abs(microtime() - $startTime) * 1000);

				if ($response->getStatusCode() == 200) {
					return $filename;
				}
			} catch (ClientErrorResponseException $e) {
				$response = $request->getResponse();
				$this->logCall($uri, 'GET', array("filename" => $filename), $response->getBody(true), abs(microtime() - $startTime) * 1000);

				if ($request->getResponse()->getStatusCode() == 201) {

					// file not ready yet do nothing

				} elseif ($request->getResponse()->getStatusCode() == 401) {

					// TT token expired
					$this->refreshToken();

				} else {
					throw new RestApiException("Error occurred while downloading file. Status code ". $response->getStatusCode(), 500, $e);
				}
			}

			sleep(self::BACKOFF_INTERVAL * ($i + 1));
		}

		return false;
	}


	/**
	 *
	 */
	private function request($uri, $method = 'GET', $params = array(), $headers = array(), $logCall = true, $refreshToken = true)
	{
		if ($refreshToken && !$this->authTt) {
			$this->refreshToken();
		}

		$startTime = microtime();
		$jsonParams = is_array($params) ? json_encode($params) : $params;

		$backOffInterval = self::BACKOFF_INTERVAL;
		$request = null;
		$response = null;
		$exception = null;
		for ($i = 0; $i < self::RETRIES_COUNT; $i++) {

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

				$duration = abs(microtime() - $startTime) * 1000;
				if ($logCall) $this->logCall($uri, $method, $params, $response->getBody(true), $duration);

				$this->logUsage($uri, $method, $params, $headers, $request, $duration);
				if ($response->isSuccessful()) {
					return $response;
				}

			} catch (\Exception $e) {
				$exception = $e;
				$duration = abs(microtime() - $startTime) * 1000;
				$response = $request->getResponse()->getBody(true);

				if ($logCall) $this->logCall($uri, $method, $params, $response, $duration);
				$this->logUsage($uri, $method, $params, $headers, $request, $duration);

				if ($e instanceof ClientErrorResponseException) {
					if ($request->getResponse()->getStatusCode() == 401) {
						// TT token expired
						$this->refreshToken();
					} else {
						$responseJson = json_decode($response, true);
						if ($responseJson !== false) {
							// Include parameters directly to error message
							if (isset($responseJson['error']) && isset($responseJson['error']['parameters']) && isset($responseJson['error']['message'])) {
								$responseJson['error']['message'] = vsprintf($responseJson['error']['message'], $responseJson['error']['parameters']);
								unset($responseJson['error']['parameters']);
							}
							$response = json_encode($responseJson);
						}
						throw new RestApiException('API error ' . $request->getResponse()->getStatusCode(), $response);
					}
				} elseif ($e instanceof ServerErrorResponseException) {
					if ($request->getResponse()->getStatusCode() == 503) {
						// Wait indefinitely due to GD maintenance
						$i--;
						$backOffInterval = 10 * 60;
					}
				} elseif ($e instanceof CurlException) {
					// Just retry according to backoff algorithm
				} else {
					throw $e;
				}

			}

			sleep($backOffInterval * ($i + 1));
		}

		/** @var $response \Guzzle\Http\Message\Response */
		$status = $request ? $request->getResponse()->getStatusCode() : null;
		$this->logAlert('GoodData API error', array(
			'uri' => $uri,
			'method' => $method,
			'params' => $jsonParams,
			'headers' => $headers,
			'response' => $response,
			'status' => $status,
			'exception' => $exception
		));
		throw new RestApiException('GoodData API error ' . $status, $response);
	}

	protected function logUsage($uri, $method, $params, $headers, RequestInterface $request, $duration)
	{
		if (!$this->usageLogger) {
			return;
		}

		$response = $request->getResponse();
		$this->usageLogger->debug($method . ' ' . $uri, array(
			'request' => array(
				'params' => $params,
				'headers' => $headers
			),
			'response' => array(
				'status' => $response->getStatusCode(),
				'body' => $response->getBody(true)
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
			throw new RestApiException('Rest API Login failed');
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


	protected function logCall($uri, $method, $params, $response, $duration)
	{
		$decodedResponse = json_decode($response, true);
		if (!$decodedResponse) {
			$decodedResponse = array($response);
		}

		$clearFromLog = $this->clearFromLog;
		$sanitize = function(&$value) use($clearFromLog) {
			if ($value && in_array((string)$value, $clearFromLog)) {
				$value = '***';
			}
		};
		array_walk_recursive($params, $sanitize);
		array_walk_recursive($decodedResponse, $sanitize);

		$this->logFile->fwrite(json_encode(array(
			'duration' => $duration,
			'time' => date('c'),
			'uri' => $this->client->getBaseUrl() . $uri,
			'method' => $method,
			'params' => $params,
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
