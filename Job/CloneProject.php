<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\GoodData\RestApiException,
	Keboola\GoodDataWriter\GoodData\UnauthorizedException;

class CloneProject extends AbstractJob
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
		$this->configuration->checkBucketAttributes();
		$bucketInfo = $this->configuration->bucketInfo();

		$gdWriteStartTime = date('c');
		try {
			// Check access to source project
			$this->restApi->setCredentials($bucketInfo['gd']['username'], $bucketInfo['gd']['password']);
			$this->restApi->getProject($bucketInfo['gd']['pid']);

			$this->restApi->setCredentials($this->mainConfig['gd']['username'], $this->mainConfig['gd']['password']);
			// Get user uri if not set
			if (empty($bucketInfo['gd']['uid'])) {
				$userId = $this->restApi->userId($bucketInfo['gd']['username'], $this->mainConfig['gd']['domain']);
				$this->configuration->updateWriter('gd.uid', $userId);
				$bucketInfo['gd']['uid'] = $userId;
			}
			$projectPid = $this->restApi->createProject($params['projectName'], $params['accessToken']);
			$this->restApi->cloneProject($bucketInfo['gd']['pid'], $projectPid,
				empty($params['includeData']) ? 0 : 1, empty($params['includeUsers']) ? 0 : 1);
			$this->restApi->addUserToProject($bucketInfo['gd']['uid'], $projectPid);

			$this->configuration->saveProject($projectPid);
			$this->sharedConfig->saveProject($projectPid, $params['accessToken'], $this->restApi->apiUrl, $job);

			return $this->_prepareResult($job['id'], array(
				'pid' => $projectPid,
				'gdWriteStartTime' => $gdWriteStartTime
			), $this->restApi->callsLog());


		} catch (UnauthorizedException $e) {
			throw new WrongConfigurationException('Rest API Login failed');
		} catch (RestApiException $e) {
			return $this->_prepareResult($job['id'], array(
				'status' => 'error',
				'error' => $e->getMessage(),
				'gdWriteStartTime' => $gdWriteStartTime
			), $this->restApi->callsLog());
		}
	}
}