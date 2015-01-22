<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-05-07
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\GoodData\RestApiException;

class ResetTable extends AbstractJob
{

	public function prepare($params)
	{
		$this->checkParams($params, array('writerId', 'tableId'));
		$this->checkWriterExistence($params['writerId']);

		return array(
			'tableId' => $params['tableId']
		);
	}

	/**
	 * required: tableId
	 * optional:
	 */
	public function run($job, $params, RestApi $restApi)
	{
		$this->checkParams($params, array('tableId'));

		$bucketAttributes = $this->configuration->bucketAttributes();

		$tableDefinition = $this->configuration->getDataSet($params['tableId']);
		$dataSetName = !empty($tableDefinition['name']) ? $tableDefinition['name'] : $tableDefinition['id'];

		$projects = $this->configuration->getProjects();

		$result = array();
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

			$restApiLogPath = null;
			if ($e instanceof RestApiException) {
				$error = $e->getDetails();
			}

			if (!($e instanceof RestApiException)) {
				throw $e;
			}

			$result['error'] = $error;
		}

		return $result;
	}
}