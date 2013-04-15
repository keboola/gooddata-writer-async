<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\GoodData\RestApiException,
	Keboola\GoodDataWriter\GoodData\UnauthorizedException;

class CloneProject extends GenericJob
{
	/**
	 * @param $job
	 * @param $params
	 * @throws WrongConfigurationException
	 * @return array
	 */
	public function run($job, $params)
	{
		if (empty($params['accessToken'])) {
			throw new WrongConfigurationException("Parameter accessToken is missing");
		}
		if (empty($params['projectName'])) {
			throw new WrongConfigurationException("Parameter projectName is missing");
		}
		if (empty($params['pidSource'])) {
			throw new WrongConfigurationException("Parameter pidSource is missing");
		}
		$this->configuration->checkGoodDataSetup();

		$env = empty($params['dev']) ? 'prod' :'dev';
		$mainConfig = $this->mainConfig['gd'][$env];


		$gdWriteStartTime = date('c');
		try {
			// Check access to source project
			$this->restApi->login($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);
			$this->restApi->getProject($this->configuration->bucketInfo['gd']['pid']);

			$this->restApi->login($mainConfig['username'], $mainConfig['password']);
			// Get user uri if not set
			if (empty($this->configuration->bucketInfo['gd']['userUri'])) {
				$userUri = $this->restApi->userUri($this->configuration->bucketInfo['gd']['username'], $mainConfig['domain']);
				$this->configuration->setBucketAttribute('gd.userUri', $userUri);
				$this->configuration->bucketInfo['gd']['userUri'] = $userUri;
			}
			$projectPid = $this->restApi->createProject($params['projectName'], $params['accessToken']);
			$this->restApi->cloneProject($this->configuration->bucketInfo['gd']['pid'], $projectPid);
			$this->restApi->addUserToProject($this->configuration->bucketInfo['gd']['userUri'], $projectPid);

			$this->configuration->saveProjectToConfiguration($projectPid);
			$this->sharedConfig->saveProject($projectPid, $params['accessToken'], $this->restApi->apiUrl, $job);

			return $this->_prepareResult($job['id'], array(
				'pid' => $projectPid,
				'gdWriteStartTime' => $gdWriteStartTime
			), $this->restApi->callsLog());


		} catch (UnauthorizedException $e) {
			throw new WrongConfigurationException('Login failed');
		} catch (RestApiException $e) {
			return $this->_prepareResult($job['id'], array(
				'status' => 'error',
				'error' => $e->getMessage(),
				'gdWriteStartTime' => $gdWriteStartTime
			), $this->restApi->callsLog());
		}
	}
}