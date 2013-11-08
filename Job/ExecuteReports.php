<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\GoodData\RestApiException,
	Keboola\GoodDataWriter\GoodData\UnauthorizedException;

class ExecuteReports extends AbstractJob
{
	/**
	 * @param $job
	 * @param $params
	 * @throws WrongConfigurationException
	 * @return array
	 */
	public function run($job, $params)
	{
		$this->configuration->checkBucketAttributes();

		$pids = array();
		if (empty($job['pid'])) {
			$projects = $this->configuration->getProjects();
			foreach ($projects as $project) if ($project['active']) {
				$pids[] = $project['pid'];
			}
		} else {
			$pids[] = $job['pid'];
		}


		$gdWriteStartTime = date('c');
		try {
			$this->restApi->setCredentials($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);
			foreach ($pids as $pid) {
				$reports = $this->restApi->get(sprintf('/gdc/md/%s/query/reports', $pid));
				if (isset($reports['query']['entries'])) {
					foreach ($reports['query']['entries'] as $report) {
						$this->restApi->executeReport($report['link']);
					}
				} else {
					throw new RestApiException('Bad format of response, missing query.entries key.');
				}
			}

			return $this->_prepareResult($job['id'], array(
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