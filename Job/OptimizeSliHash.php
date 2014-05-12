<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2014-05-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\GoodDataWriter\GoodData\RestApi;

class OptimizeSliHash extends AbstractJob
{
	/**
	 * required: email, role
	 * optional: pid
	 */
	public function run($job, $params)
	{
		$gdWriteStartTime = time();

		$goodDataModel = new Model($this->appConfiguration);
		$manifests = array();
		$dataSetsToOptimize = array();
		foreach ($this->configuration->getDataSets() as $dataSet) if ($dataSet['isExported']) {
			$definition = $this->configuration->getDataSetDefinition($dataSet['id']);
			$manifests[] = Model::getDataLoadManifest($definition, false);
			$dataSetsToOptimize[] = Model::getDatasetId($definition['name']);
		}
		foreach ($this->configuration->getDateDimensions() as $dimension) if ($dimension['includeTime'] && $dimension['isExported']) {
			$manifests[] = json_decode($goodDataModel->getTimeDimensionDataLoadManifest($dimension['name']), true);
			$dataSetsToOptimize[] = Model::getTimeDimensionId($dimension['name']);
		}


		$bucketAttributes = $this->configuration->bucketAttributes();
		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

		// Check if we have all manifests
		foreach ($this->restApi->getDataSets($bucketAttributes['gd']['pid']) as $ds) {
			if (substr($ds['id'], -3) != '.dt') {
				if (!in_array($ds['id'], $dataSetsToOptimize)) {
					throw new WrongConfigurationException(sprintf("DataSet '%s' (%s) from project does not exist in writer's configuration", $ds['title'], $ds['id']));
				}
			}
		}

		$this->restApi->optimizeSliHash($bucketAttributes['gd']['pid'], $manifests);

		$this->logEvent('optimizeSliHash', array(
			'duration' => time() - strtotime($gdWriteStartTime)
		), $this->restApi->getLogPath());
		return array(
			'gdWriteStartTime' => $gdWriteStartTime
		);
	}
}