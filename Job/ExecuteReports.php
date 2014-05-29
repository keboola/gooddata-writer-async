<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongParametersException,
	Keboola\GoodDataWriter\GoodData\RestApiException;

class ExecuteReports extends AbstractJob
{
	/**
	 * required:
	 * optional: pid, reports
	 */
	public function run($job, $params)
	{
		$this->configuration->checkBucketAttributes();
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
			if (!empty($params['reports'])) {
				foreach ((array) $params['reports'] AS $reportUri) {
					if (!preg_match('/^\/gdc\/md\/' . $project['pid'] . '\//', $reportUri)) {
						throw new WrongParametersException($this->translator->trans('parameters.report.not_valid %1', array('%1' => $reportUri)));
					}
				}
			}
		}

		$gdWriteStartTime = date('c');

		$bucketAttributes = $this->configuration->bucketAttributes();
		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
		foreach ($pids as $pid) {
			if (!empty($params['pid']) && !empty($params['reports'])) {
				// specified reports
				foreach ($params['reports'] as $reportUri) {
					$this->restApi->executeReport($reportUri);
				}
			} else {
				// all reports
				$reports = $this->restApi->get(sprintf('/gdc/md/%s/query/reports', $pid));
				if (isset($reports['query']['entries'])) {
					foreach ($reports['query']['entries'] as $report) {
						$this->restApi->executeReport($report['link']);
					}
				} else {
					throw new RestApiException($this->translator->trans('rest_api.reports_list_bad_response'));
				}
			}
		}

		$this->logEvent('executeReports', array(
			'duration' => time() - strtotime($gdWriteStartTime)
		), $this->restApi->getLogPath());
		return array(
			'gdWriteStartTime' => $gdWriteStartTime
		);
	}
}