<?php
/**
 *
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-19
 *
 */

namespace Keboola\GoodDataWriter\GoodData;

use Guzzle\Http\Client,
	Guzzle\Http\Exception\ClientErrorResponseException,
	Guzzle\Common\Exception\RuntimeException,
	Guzzle\Http\Message\Header;
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


	public $apiUrl;

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
		$this->_client->setDefaultHeaders(array(
			'accept' => 'application/json',
			'content-type' => 'application/json; charset=utf-8'
		));

		$this->_callsLog = array();
		$this->_clearFromLog = array();
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
		try {
			$result = $this->_jsonRequest(sprintf('/gdc/projects/%s', $pid), 'GET', array(), array(), false);
			return $result;
		} catch (RestApiException $e) {
			$errorJson = json_decode($e->getMessage(), true);
			if ($errorJson) {
				if (isset($errorJson['error']['errorClass'])) {
					switch ($errorJson['error']['errorClass']) {
						case 'GDC::Exception::Forbidden':
							throw new RestApiException(sprintf('Access to project %s denied', $pid));
							break;
						case 'GDC::Exception::NotFound':
							throw new RestApiException(sprintf('Project %s not exists', $pid));
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
			sleep(self::BACKOFF_INTERVAL * ($i + 1));

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
	 * @throws RestApiException
	 * @return bool
	 */
	public function cloneProject($pidSource, $pidTarget)
	{
		$uri = sprintf('/gdc/md/%s/maintenance/export', $pidSource);
		$params = array(
			'exportProject' => array(
				'exportUsers' => 0,
				'exportData' => 0
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
	 * @return null
	 */
	public function createUserInDomain($domain, $login, $password, $firstName, $lastName, $ssoProvider)
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
			$userUri = $this->userUri($login, $domain);
			if ($userUri) {
				return $userUri;
			} else {
				$this->_log->alert('createUserInDomain() failed', array(
					'uri' => $uri,
					'params' => $params,
					'exception' => $e
				));
				throw $e;
			}
		}

		if (isset($result['uri'])) {
			return $result['uri'];
		} else {
			$this->_log->alert('createUserInDomain() failed', array(
				'uri' => $uri,
				'params' => $params,
				'result' => $result
			));
			throw new RestApiException('Create user failed');
		}
	}

	/**
	 * Retrieves user's uri
	 *
	 * @param $email
	 * @param $domain
	 * @return array
	 */
	public function userUri($email, $domain)
	{
		foreach($this->usersInDomain($domain) as $user) {
			if (!empty($user['accountSetting']['login']) && $user['accountSetting']['login'] == $email) {
				return !empty($user['accountSetting']['links']['self']) ? $user['accountSetting']['links']['self'] : false;
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
	 * @param $userUri
	 * @param $pid
	 * @param string $role
	 * @throws \Exception|\Guzzle\Http\Exception\ClientErrorResponseException
	 * @throws RestApiException
	 */
	public function addUserToProject($userUri, $pid, $role = 'adminRole')
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
							'self' => $userUri
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
	 * Poll task uri and wait for its finish
	 * @param $uri
	 * @throws RestApiException
	 */
	public function waitForTask($uri)
	{
		$repeat = true;
		$i = 0;
		do {
			sleep(self::BACKOFF_INTERVAL * ($i + 1));

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
			return $this->_request($uri, $method, $params, $headers, $logCall)->json();
		} catch (RuntimeException $e) {
			$this->_log->alert('API error - bad response', array(
				'uri' => $uri,
				'method' => $method,
				'params' => $params,
				'headers' => $headers,
				'exception' => $e
			));
			throw new RestApiException('Rest API: ' . $e->getMessage());
		}
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

		for ($i = 0; $i < self::RETRIES_COUNT; $i++) {

			switch ($method) {
				case 'GET':
					$request = $this->_client->get($uri, $headers, $jsonParams);
					break;
				case 'POST':
					$request = $this->_client->post($uri, $headers, $jsonParams);
					break;
				case 'PUT':
					$request = $this->_client->put($uri, $headers, $jsonParams);
					break;
				case 'DELETE':
					$request = $this->_client->delete($uri, $headers, $jsonParams);
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
			} catch (ClientErrorResponseException $e) {
				$response = $request->getResponse()->getBody(true);
				if ($logCall) $this->_logCall($uri, $method, $params, $response);
				if ($request->getResponse()->getStatusCode() == 401) {
					throw new UnauthorizedException($response);
				}
				throw new RestApiException($response);
			}

			if ($response->isSuccessful()) {
				return $response;
			}

			if ($response->getStatusCode() != 503) {
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

			sleep(self::BACKOFF_INTERVAL * ($i + 1));
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
	public function login($login, $password)
	{
		try {
			$response = $this->_request('/gdc/account/login', 'POST', array(
				'postUserLogin' => array(
					'login' => $login,
					'password' => $password,
					'remember' => 0
				)
			), array(), false);
		} catch (RestApiException $e) {
			throw new RestApiException('Login failed');
		}

		$this->_authSst = $this->_findCookie($response, 'GDCAuthSST');
		if (!$this->_authSst) {
			$this->_log->alert('Invalid login - GDCAuthSST not found', array(
				'response' => $response->getBody(true),
				'status' => $response->getStatusCode()
			));
			throw new RestApiException('Login failed');
		}

		try {
			$response = $this->_request('/gdc/account/token', 'GET', array(), array(), false);
		} catch (RestApiException $e) {
			throw new RestApiException('Login failed');
		}

		$this->_authTt = $this->_findCookie($response, 'GDCAuthTT');
		if (!$this->_authTt) {
			$this->_log->alert('Invalid login - GDCAuthTT not found', array(
				'response' => $response->getBody(true),
				'status' => $response->getStatusCode()
			));
			throw new RestApiException('Login failed');
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