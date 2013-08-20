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
	Guzzle\Common\Exception\RuntimeException,
	Guzzle\Http\Message\Header;
use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\GoodDataWriter\GoodData\RestApiException,
	Keboola\GoodDataWriter\GoodData\UnauthorizedException;

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


	public $apiUrl;

	private $_username;
	private $_password;

	public static $userRoles = array(
		'admin' => 'adminRole',
		'editor' => 'editorRole',
		'readOnly' => 'readOnlyUserRole',
		'dashboardOnly' => 'dashboardOnlyRole'
	);

	/**
	 * @var Client
	 */
	protected $_client;
	/**
	 * @var \Monolog\Logger
	 */
	protected $_log;
	protected $_authSst;
	protected $_authTt;

	private $_callsLog;
	private $_clearFromLog;

	public function __construct($apiUrl = null, $log)
	{
		$this->_log = $log;

		if (!$apiUrl) {
			$apiUrl = 'https://secure.gooddata.com';
		} else {
			if (substr($apiUrl, 0, 8) != 'https://') {
				$apiUrl = 'https://' . $apiUrl;
			}
		}
		$this->apiUrl = $apiUrl;

		$this->_client = new Client($apiUrl, array(
			'curl.options' => array(
				CURLOPT_CONNECTTIMEOUT => 10000
			)
		));
		$this->_client->setSslVerification(false);
		$this->_client->setDefaultOption('headers', array(
			'accept' => 'application/json',
			'content-type' => 'application/json; charset=utf-8'
		));

		$this->_callsLog = array();
		$this->_clearFromLog = array();
	}

	public function setCredentials($username, $password)
	{
		$this->_username = $username;
		$this->_password = $password;
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
			$result = $this->_jsonRequest($uri, 'GET', array(), array(), false);
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
		$this->_clearFromLog[] = $authToken;

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
		$result = $this->_jsonRequest($uri, 'POST', $params);

		if (empty($result['uri']) || strpos($result['uri'], '/gdc/projects/') === false) {
			$this->_log->alert('createProject() failed', array(
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

			$result = $this->_jsonRequest($projectUri);
			if (isset($result['project']['content']['state'])) {
				if ($result['project']['content']['state'] == 'ENABLED') {
					$repeat = false;
				}
			} else {
				$this->_log->alert('createProject() failed', array(
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
		return $this->_jsonRequest($uri, 'DELETE');
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
		$result = $this->_jsonRequest($uri, 'POST', $params);
		if (empty($result['exportArtifact']['token']) || empty($result['exportArtifact']['status']['uri'])) {
			$this->_log->alert('cloneProject() export failed', array(
				'uri' => $uri,
				'params' => $params,
				'result' => $result
			));
			throw new RestApiException('Clone export failed');
		}

		$this->waitForTask($result['exportArtifact']['status']['uri']);

		$result = $this->_jsonRequest(sprintf('/gdc/md/%s/maintenance/import', $pidTarget), 'POST', array(
			'importProject' => array(
				'token' => $result['exportArtifact']['token']
			)
		));
		if (empty($result['uri'])) {
			$this->_log->alert('cloneProject() import failed', array(
				'uri' => $uri,
				'params' => $params,
				'result' => $result
			));
			throw new RestApiException('Clone import failed');
		}

		$this->waitForTask($result['uri']);
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
		$this->_clearFromLog[] = $password;

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
			$result = $this->_jsonRequest($uri, 'POST', $params);
		} catch (RestApiException $e) {
			// User exists?
			$userId = $this->userId($login, $domain);
			if ($userId) {
				return $userId;
			} else {
				$this->_log->alert('createUser() failed', array(
					'uri' => $uri,
					'params' => $params,
					'exception' => $e
				));
				throw $e;
			}
		}

		if (isset($result['uri'])) {
			if (substr($result['uri'], 0, 21) == '/gdc/account/profile/') {
				return substr($result['uri'], 21);
			} else {
				$this->_log->alert('createUser() has wrong result', array(
					'uri' => $uri,
					'params' => $params,
					'result' => $result
				));
			}
		} else {
			$this->_log->alert('createUser() failed', array(
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
		return $this->_jsonRequest('/gdc/account/profile/' . $uid, 'DELETE');
	}

	/**
	 * Retrieves user's uri
	 *
	 * @param $email
	 * @param $domain
	 * @return array
	 */
	public function userId($email, $domain)
	{
		foreach($this->usersInDomain($domain) as $user) {
			if (!empty($user['accountSetting']['login']) && $user['accountSetting']['login'] == $email) {
				if (!empty($user['accountSetting']['links']['self'])) {
					if (substr($user['accountSetting']['links']['self'], 0, 21) == '/gdc/account/profile/') {
						return substr($user['accountSetting']['links']['self'], 21);
					} else {
						$this->_log->alert('userId() has wrong result', array(
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
		$result = $this->_jsonRequest(sprintf('/gdc/account/domains/%s/users', $domain), 'GET', array(), array(), false);
		return isset($result['accountSettings']['items']) ? $result['accountSettings']['items'] : array();
	}

	/**
	 * Retrieves list of all users in the project
	 *
	 * @param $pid
	 * @return array
	 */
	public function usersInProject($pid)
	{
		$result = $this->_jsonRequest(sprintf('/gdc/account/projects/%s/users', $pid));
		return isset($result['accountSettings']['items']) ? $result['accountSettings']['items'] : array();
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
		$rolesUri = sprintf('/gdc/projects/%s/roles', $pid);
		$rolesResult = $this->_jsonRequest($rolesUri);
		$projectRoleUri = '';
		if (isset($rolesResult['projectRoles']['roles'])) {
			foreach($rolesResult['projectRoles']['roles'] as $roleUri) {
				$roleResult = $this->_jsonRequest($roleUri);
				if (isset($roleResult['projectRole']['meta']['identifier']) && $roleResult['projectRole']['meta']['identifier'] == $role) {
					$projectRoleUri = $roleUri;
					break;
				}
			}

			if ($projectRoleUri) {

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
				$result = $this->_jsonRequest($uri, 'POST', $params);

				if ((isset($result['projectUsersUpdateResult']['successful']) && count($result['projectUsersUpdateResult']['successful']))
					|| (isset($result['projectUsersUpdateResult']['failed']) && !count($result['projectUsersUpdateResult']['failed']))) {
					// SUCCESS
					// Sometimes API does not return
				} else {
					$this->_log->alert('addUserToProject() has not added user to project', array(
						'uri' => $uri,
						'params' => $params,
						'result' => $result
					));
					throw new RestApiException('Error in addition to project');
				}

			} else {
				$this->_log->alert('addUserToProject() has not found role in project', array(
					'role' => $role,
					'result' => $rolesResult
				));
				throw new RestApiException('Role in project not found');
			}
		} else {
			$this->_log->alert('addUserToProject() has bad response', array(
				'uri' => $rolesUri,
				'result' => $rolesResult
			));
			throw new RestApiException('Roles in project could not be fetched');
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
		$rolesUri = sprintf('/gdc/projects/%s/roles', $pid);
		$rolesResult = $this->_jsonRequest($rolesUri);
		$projectRoleUri = '';
		if (isset($rolesResult['projectRoles']['roles'])) {
			foreach($rolesResult['projectRoles']['roles'] as $roleUri) {
				$roleResult = $this->_jsonRequest($roleUri);
				if (isset($roleResult['projectRole']['meta']['identifier']) && $roleResult['projectRole']['meta']['identifier'] == $role) {
					$projectRoleUri = $roleUri;
					break;
				}
			}

			if ($projectRoleUri) {

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
				$result = $this->_jsonRequest($uri, 'POST', $params);

				if (isset($result['createdInvitations']['uri']) && count($result['createdInvitations']['uri'])) {
					// SUCCESS
				} else {
					$this->_log->alert('inviteUserToProject() has not invited user to project', array(
						'uri' => $uri,
						'params' => $params,
						'result' => $result
					));
					throw new RestApiException('Error in invitation to project');
				}

			} else {
				$this->_log->alert('inviteUserToProject() has not found role in project', array(
					'role' => $role,
					'result' => $rolesResult
				));
				throw new RestApiException('Role in project not found');
			}
		} else {
			$this->_log->alert('inviteUserToProject() has bad response', array(
				'uri' => $rolesUri,
				'result' => $rolesResult
			));
			throw new RestApiException('Roles in project could not be fetched');
		}
	}



	/**
	 * Run load data task
	 *
	 * @param $pid
	 * @param string $dirName
	 * @return array|bool|float|int|string
	 * @throws \Exception|\Guzzle\Http\Exception\ClientErrorResponseException
	 * @throws RestApiException
	 */
	public function loadData($pid, $dirName)
	{
		$uri = sprintf('/gdc/md/%s/etl/pull', $pid);
		$result = $this->_jsonRequest($uri, 'POST', array('pullIntegration' => $dirName));

		if (isset($result['pullTask']['uri'])) {

			$try = 1;
			do {
				sleep(10 * $try);
				$taskResponse = $this->_jsonRequest($result['pullTask']['uri']);

				if (!isset($taskResponse['taskStatus'])) {
					$this->_log->alert('loadData() has bad response', array(
						'uri' => $result['pullTask']['uri'],
						'result' => $taskResponse
					));
					throw new RestApiException('ETL task could not be checked');
				}

				$try++;
			} while (in_array($taskResponse['taskStatus'], array('PREPARED', 'RUNNING')));

			return $taskResponse;

		} else {
			$this->_log->alert('loadData() has bad response', array(
				'uri' => $uri,
				'result' => $result
			));
			throw new RestApiException('Pull task could not be started');
		}
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
		$this->_requestWithLogin('/gdc/xtab2/executor3', 'POST', array(
			'report_req' => array(
				'report' => $uri
			)
		));
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
		$identifier = str_replace(' ', '', strtolower($name));
		$maql = sprintf('INCLUDE TEMPLATE "URN:GOODDATA:DATE" MODIFY (IDENTIFIER "%s", TITLE "%s");', $identifier, $name);

		if ($includeTime) {
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

		$this->_requestWithLogin(sprintf('/gdc/md/%s/ldm/manage', $pid), 'POST', array(
			'manage' => array(
				'maql' => $maql
			)
		));
	}




	/**
	 * Execute MAQL
	 * @param $pid
	 * @param $maql
	 * @return string
	 */
	public function executeMaql($pid, $maql)
	{
		$uri = sprintf('/gdc/md/%s/ldm/manage', $pid);
		$params = array(
			'manage' => array(
				'maql' => $maql
			)
		);
		return $this->_jsonRequest($uri, 'POST', $params);
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
		$gdAttribute = $this->getAttributeByTitle($pid, $attribute);

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
		$result = $this->_jsonRequest($filterUri, 'POST', array(
			'userFilter' => array(
				'content' => array(
					'expression' => $expression
				),
				'meta' => array(
					'category'  => 'userFilter',
					'title'     => $name
				)
			)
		));

		if (isset($result['uri'])) {
			return $result['uri'];
		} else {
			$this->_log->alert('createFilters() has bad response', array(
				'uri' => $filterUri,
				'result' => $result
			));
			throw new RestApiException('Error in attempt to create filter');
		}
	}

	public function assignFiltersToUser(array $filters, $userId, $pid)
	{
		$uri = sprintf('/gdc/md/%s/userfilters', $pid);

		$result = $this->_jsonRequest($uri, 'POST', array(
			'userFilters' => array(
				'items' => array(
					array(
						"user" => "/gdc/account/profile/" . $userId,
						"userFilters" => $filters
					)
				)
			)
		));

		if (isset($result['userFiltersUpdateResult']['successful']) && count($result['userFiltersUpdateResult']['successful'])) {
			// SUCCESS
		} else {
			$this->_log->alert('assignFiltersToUser() has bad response', array(
				'uri' => $uri,
				'result' => $result
			));
			throw new RestApiException('Error in attempt to assign filters to user');
		}
	}

	public function deleteFilter($filterUri)
	{
		$this->_jsonRequest($filterUri, 'DELETE');
	}

	public function getFilters($pid)
	{
		$uri = sprintf('/gdc/md/%s/query/userfilters', $pid);
		$result = $this->_jsonRequest($uri);

		if (isset($result['query']['entries'])) {
			return $result['query']['entries'];
		} else {
			$this->_log->alert('getFilters() has bad response', array(
				'uri' => $uri,
				'result' => $result
			));
			throw new RestApiException('Filters in project could not be fetched');
		}
	}

	public function getAttributes($pid)
	{
		$uri = sprintf('/gdc/md/%s/query/attributes', $pid);
		$result = $this->_jsonRequest($uri);

		if (isset($result['query']['entries'])) {
			return $result['query']['entries'];
		} else {
			$this->_log->alert('getAttributes() has bad response', array(
				'uri' => $uri,
				'result' => $result
			));
			throw new RestApiException('Attributes in project could not be fetched');
		}
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
			$this->_log->alert('getAttributeByTitle() attribute not found', array(
				'pid' => $pid,
				'attribute' => $title
			));
			throw new RestApiException('Attribute ' . $title . ' not found in project');
		} else {
			$result = $this->_jsonRequest($attrUri);
			if (isset($result['attribute'])) {
				return $result['attribute'];
			} else {
				$this->_log->alert('getAttributeByTitle() bad response', array(
					'uri' => $attrUri,
					'result' => $result
				));
				throw new RestApiException('Attribute ' . $attrUri . ' could not be fetched');
			}
		}
	}

	public function getElements($uri)
	{
		$result = $this->_jsonRequest($uri);
		if (isset($result['attributeElements']['elements'])) {
			return $result['attributeElements']['elements'];
		} else {
			$this->_log->alert('getElements() has bad response', array(
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
			$this->_log->alert('getElementsByTitle() element not found', array(
				'uri' => $uri,
				'element' => $title
			));
			throw new RestApiException('Element ' . $title . ' not found');
		} else {
			return $elementUri;
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

			$result = $this->_jsonRequest($uri);
			if (isset($result['taskState']['status'])) {
				if (in_array($result['taskState']['status'], array('OK', 'ERROR', 'WARNING'))) {
					$repeat = false;
				}
			} else {
				$this->_log->alert('waitForTask() has bad response', array(
					'uri' => $uri,
					'result' => $result
				));
				throw new RestApiException('Bad response');
			}

			$i++;
		} while ($repeat);

		if ($result['taskState']['status'] != 'OK') {
			$this->_log->alert('waitForTask() has bad response', array(
				'uri' => $uri,
				'result' => $result
			));
			throw new RestApiException('Bad response');
		}
	}


	/**
	 * Common request expecting json result
	 * @param $uri
	 * @param string $method
	 * @param array $params
	 * @param array $headers
	 * @param bool $logCall
	 * @throws RestApiException
	 * @return array|bool|float|int|string
	 */
	private function _jsonRequest($uri, $method = 'GET', $params = array(), $headers = array(), $logCall = true)
	{
		try {
			return $this->_requestWithLogin($uri, $method, $params, $headers, $logCall)->json();
		} catch (RuntimeException $e) {
			$this->_log->alert('API error - bad response', array(
				'uri' => $uri,
				'method' => $method,
				'params' => $params,
				'headers' => $headers,
				'exception' => array($e->getMessage())
			));
			throw new RestApiException('Rest API: ' . $e->getMessage());
		}
	}


	public function _requestWithLogin($uri, $method = 'GET', $params = array(), $headers = array(), $logCall = true)
	{
		$this->login();
		return $this->_request($uri, $method, $params, $headers, $logCall);
	}

	/**
	 * @param $uri
	 * @param string $method
	 * @param array $params
	 * @param array $headers
	 * @param bool $logCall
	 * @throws RestApiException
	 * @throws UnauthorizedException
	 * @return \Guzzle\Http\Message\Response
	 */
	private function _request($uri, $method = 'GET', $params = array(), $headers = array(), $logCall = true)
	{
		$jsonParams = is_array($params) ? json_encode($params) : $params;

		$backoffInterval = self::BACKOFF_INTERVAL;
		$error401 = false;
		$response = null;
		for ($i = 0; $i < self::RETRIES_COUNT; $i++) {

			switch ($method) {
				case 'GET':
					$request = $this->_client->get($uri, $headers);
					break;
				case 'POST':
					$request = $this->_client->post($uri, $headers, $jsonParams);
					break;
				case 'PUT':
					$request = $this->_client->put($uri, $headers, $jsonParams);
					break;
				case 'DELETE':
					$request = $this->_client->delete($uri, $headers);
					break;
				default:
					throw new RestApiException('Unsupported request method');
			}
			if ($this->_authSst) {
				$request->addCookie('GDCAuthSST', $this->_authSst);
			}
			if ($this->_authTt) {
				$request->addCookie('GDCAuthTT', $this->_authTt);
			}

			try {
				$response = $request->send();
				if ($logCall) $this->_logCall($uri, $method, $params, $response->getBody(true));
				$this->_log->debug(array(
					'baseUrl' => $this->_client->getBaseUrl(),
					'uri' => $uri,
					'method' => $method,
					'params' => $params,
					'headers' => $headers,
					'response' => $response->getBody(true)
				));

				if ($response->isSuccessful()) {
					return $response;
				}

			} catch (ClientErrorResponseException $e) {
				$response = $request->getResponse()->getBody(true);
				if ($logCall) $this->_logCall($uri, $method, $params, $response);
				if ($request->getResponse()->getStatusCode() == 401) {
					$error401 = true;
				} else {
					throw new RestApiException($response);
				}
			} catch (ServerErrorResponseException $e) {
				// Backoff
				if ($request->getResponse()->getStatusCode() == 503) {
					// Wait indefinitely
					$i--;
					$backoffInterval = 10 * 60;
				}
				$error401 = false;
			}

			sleep($backoffInterval * ($i + 1));
		}

		if ($error401) {
			throw new UnauthorizedException($response);
		}

		/** @var $response \Guzzle\Http\Message\Response */
		$this->_log->alert('API error', array(
			'uri' => $uri,
			'method' => $method,
			'params' => $jsonParams,
			'headers' => $headers,
			'response' => $response->getBody(true),
			'status' => $response->getStatusCode()
		));
		throw new RestApiException($response->getBody(true));
	}

	/**
	 * @param $login
	 * @param $password
	 * @throws RestApiException
	 */
	public function login($login = null, $password = null)
	{
		if (!$login) $login = $this->_username;
		if (!$password) $password = $this->_password;

		try {
			$response = $this->_request('/gdc/account/login', 'POST', array(
				'postUserLogin' => array(
					'login' => $login,
					'password' => $password,
					'remember' => 0
				)
			), array(), false);
		} catch (RestApiException $e) {
			throw new RestApiException('Rest API Login failed');
		}

		$this->_authSst = $this->_findCookie($response, 'GDCAuthSST');
		if (!$this->_authSst) {
			$this->_log->alert('Invalid login - GDCAuthSST not found', array(
				'response' => $response->getBody(true),
				'status' => $response->getStatusCode()
			));
			throw new RestApiException('Rest API Login failed');
		}

		try {
			$response = $this->_request('/gdc/account/token', 'GET', array(), array(), false);
		} catch (RestApiException $e) {
			throw new RestApiException('Rest API Login failed');
		}

		$this->_authTt = $this->_findCookie($response, 'GDCAuthTT');
		if (!$this->_authTt) {
			$this->_log->alert('Invalid login - GDCAuthTT not found', array(
				'response' => $response->getBody(true),
				'status' => $response->getStatusCode()
			));
			throw new RestApiException('Rest API Login failed');
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
	protected function _findCookie($response, $name)
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

	protected function _logCall($uri, $method, $params, $response)
	{
		$decodedResponse = json_decode($response, true);
		if (!$decodedResponse) {
			$decodedResponse = array($response);
		}

		$clearFromLog = $this->_clearFromLog;
		$sanitize = function(&$value) use($clearFromLog) {
			if (in_array($value, $clearFromLog)) {
				$value = '---';
			}
		};
		array_walk_recursive($params, $sanitize);
		array_walk_recursive($decodedResponse, $sanitize);

		$this->_callsLog[] = array(
			'timestamp' => date('c'),
			'backendUrl' => $this->_client->getBaseUrl(),
			'uri' => $uri,
			'method' => $method,
			'params' => $params,
			'response' => $decodedResponse
		);
	}

	public function callsLog()
	{
		return json_encode($this->_callsLog);
	}
}
