<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-05-07
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\GoodData\RestApiException;
use Symfony\Component\Stopwatch\Stopwatch;

class ResetTable extends AbstractJob
{
	/**
	 */
	public function run($job, $params)
	{
		$this->checkParams($params, array('tableId'));

		$this->logEvent('start', array(
			'duration' => 0
		));
		$stopWatch = new Stopwatch();
		$stopWatchId = 'prepareJob';
		$stopWatch->start($stopWatchId);

		$bucketAttributes = $this->configuration->bucketAttributes();
		$this->configuration->checkBucketAttributes();

		$tableDefinition = $this->configuration->getDataSet($params['tableId']);
		$dataSetName = !empty($tableDefinition['name']) ? $tableDefinition['name'] : $tableDefinition['id'];

		$projects = $this->configuration->getProjects();
		$gdWriteStartTime = date('c');

		$e = $stopWatch->stop($stopWatchId);
		$this->logEvent($stopWatchId, array(
			'duration' => $e->getDuration()
		), $this->restApi->getLogPath());
		$this->restApi->initLog();

		$result = array();
		$stopWatchId = 'restApi';
		$stopWatch->start($stopWatchId);
		try {
			$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
			foreach ($projects as $project) if ($project['active']) {
				$this->restApi->dropDataSet($project['pid'], $dataSetName);
			}

			$this->configuration->updateDataSetDefinition($params['tableId'], 'isExported', 0);
		} catch (\Exception $e) {
			$error = $e->getMessage();
			$event = $stopWatch->stop($stopWatchId);

			$restApiLogPath = null;
			$eventDetail = array(
				'duration' => $event->getDuration()
			);
			if ($e instanceof RestApiException) {
				$error = $e->getDetails();
				$restApiLogPath = $this->restApi->getLogPath();
			}
			$this->logEvent($stopWatchId, $eventDetail, $restApiLogPath);

			if (!($e instanceof RestApiException)) {
				throw $e;
			}

			$result['error'] = $error;
		}

		$result['gdWriteStartTime'] = $gdWriteStartTime;
		return $result;
	}
}