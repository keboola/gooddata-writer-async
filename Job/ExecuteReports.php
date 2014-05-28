<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\GoodDataWriter\Exception\WrongParametersException,
	Keboola\GoodDataWriter\GoodData\RestApiException;

class ExecuteReports extends AbstractJob
{
	/**
	 * required: pid
	 * optional: reports
	 */
	public function run($job, $params)
	{
		$this->checkParams($params, array('pid'));
		$this->configuration->checkBucketAttributes();
		$this->configuration->checkProjectsTable();

		$project = $this->configuration->getProject($params['pid']);
		if (!$project) {
			throw new WrongConfigurationException($this->translator->trans('parameters.pid_not_configured'));
		}

		// reports uri validation if pid was specified
		if (!empty($params['reports'])) {
			foreach ((array) $params['reports'] AS $reportUri) {
				if (!preg_match('/^\/gdc\/md\/' . $project['pid'] . '\//', $reportUri)) {
					throw new WrongParametersException($this->translator->trans('parameters.report.not_valid %1', array('%1' => $reportUri)));
				}
			}
		}

		$gdWriteStartTime = date('c');

		$bucketAttributes = $this->configuration->bucketAttributes();
		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
		if (!empty($params['reports'])) {
			// specified reports
			foreach ($params['reports'] as $reportUri) {
				$this->restApi->executeReport($reportUri);
			}
		} else {
			// all reports
			$reports = $this->restApi->get(sprintf('/gdc/md/%s/query/reports', $params['pid']));
			if (isset($reports['query']['entries'])) {
				foreach ($reports['query']['entries'] as $report) {
					$this->restApi->executeReport($report['link']);
				}
			} else {
				throw new RestApiException('Bad format of response, missing query.entries key.');
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