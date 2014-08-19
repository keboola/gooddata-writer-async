<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongParametersException,
	Keboola\GoodDataWriter\GoodData\RestApiException;
use Keboola\GoodDataWriter\GoodData\RestApi;

class ExecuteReports extends AbstractJob
{
	/**
	 * required: pid
	 * optional: reports
	 */
	public function run($job, $params, RestApi $restApi)
	{
		$this->checkParams($params, array('pid'));
		$project = $this->configuration->getProject($params['pid']);
		if (!$project) {
			throw new WrongParametersException($this->translator->trans('parameters.pid_not_configured'));
		}
		$bucketAttributes = $this->configuration->bucketAttributes();
		$this->configuration->checkBucketAttributes($bucketAttributes);
		$this->configuration->checkProjectsTable();

		// reports uri validation
		if (!empty($params['reports'])) {
			foreach ((array) $params['reports'] AS $reportUri) {
				if (!preg_match('/^\/gdc\/md\/' . $project['pid'] . '\//', $reportUri)) {
					throw new WrongParametersException($this->translator->trans('parameters.report.not_valid %1', array('%1' => $reportUri)));
				}
			}
		}

		$gdWriteStartTime = date('c');

		$restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
			if (!empty($params['reports'])) {
				// specified reports
				foreach ($params['reports'] as $reportUri) {
					$restApi->executeReport($reportUri);
				}
			} else {
				// all reports
				$reports = $restApi->get(sprintf('/gdc/md/%s/query/reports', $params['pid']));
				if (isset($reports['query']['entries'])) {
					foreach ($reports['query']['entries'] as $report) {
						$restApi->executeReport($report['link']);
					}
				} else {
					throw new RestApiException($this->translator->trans('rest_api.reports_list_bad_response'));
				}
			}

		$this->logEvent('execute_reports', array(
			'duration' => time() - strtotime($gdWriteStartTime)
		), $restApi->getLogPath());
		return array(
			'gdWriteStartTime' => $gdWriteStartTime
		);
	}
}