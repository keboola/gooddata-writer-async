<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-05-07
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\GoodData\RestApiException;
use Symfony\Component\Stopwatch\Stopwatch;

class ResetTable extends AbstractJob
{
	/**
	 * required: tableId
	 * optional:
	 */
	public function run($job, $params, RestApi $restApi)
	{
		$this->checkParams($params, array('tableId'));

		$this->logEvent('start', array(
			'duration' => 0
		));
		$stopWatch = new Stopwatch();
		$stopWatchId = 'prepareJob';
		$stopWatch->start($stopWatchId);

		$bucketAttributes = $this->configuration->bucketAttributes();
		$this->configuration->checkBucketAttributes($bucketAttributes);

		$tableDefinition = $this->configuration->getDataSet($params['tableId']);
		$dataSetName = !empty($tableDefinition['name']) ? $tableDefinition['name'] : $tableDefinition['id'];

		$projects = $this->configuration->getProjects();
		$gdWriteStartTime = date('c');

		$e = $stopWatch->stop($stopWatchId);
		$this->logEvent($stopWatchId, array(
			'duration' => $e->getDuration()
		), $restApi->getLogPath());
		$restApi->initLog();

		$result = array();
		$stopWatchId = 'restApi';
		$stopWatch->start($stopWatchId);
		try {
			$restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

			$updateOperations = array();
			foreach ($projects as $project) if ($project['active']) {
				$result = $restApi->dropDataSet($project['pid'], $dataSetName);
				if ($result) {
					$updateOperations[$project['pid']] = $result;
				}
			}
			if (count($updateOperations)) {
				$result['info'] = $updateOperations;
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
				$restApiLogPath = $restApi->getLogPath();
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