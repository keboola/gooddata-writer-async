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
		$this->configuration->checkGoodDataSetup();
		$this->configuration->checkProjectsTable();

		$pids = array();
		if (empty($params['pid'])) {
			$projects = $this->configuration->getProjects();
			foreach ($projects as $project) if ($project['active']) {
				$pids[] = $project['pid'];
			}
		} else {
			$project = $this->configuration->getProject($params['pid']);
			if ($project && $project['active']) {
				$pids[] = $project['pid'];
			}

			// reports uri validation if pid was specified
			if (empty($params['reports'])) {
				foreach ((array) $params['reports'] AS $reportLink) {
					if (!preg_match('/^\/gdc\/md\/' . $project['pid'] . '\//', $reportLink)) {
						throw new WrongParametersException("Parameter 'reports' is not valid; report uri '" .$reportLink . "' does not belong to the project");
					}
				}
			}
		}

		$gdWriteStartTime = date('c');

		try {
			$this->restApi->setCredentials($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);
			foreach ($pids as $pid) {
				if (!empty($params['pid']) && !empty($params['reports'])) {
					// specified reports
					foreach ($params['reports'] as $reportLink) {
						$this->restApi->executeReport($reportLink);
					}
				} else {
					// all reports
					$reports = $this->restApi->get(sprintf('/gdc/md/%s/query/reports', $pid));
					if (isset($reports['query']['entries'])) {
						foreach ($reports['query']['entries'] as $report) {
							$this->restApi->executeReport($report['link']);
						}
					} else {
						throw new RestApiException('Bad format of response, missing query.entries key.');
					}
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