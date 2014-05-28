<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2014-05-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\GoodDataWriter\Writer\SharedConfig;

class OptimizeSliHash extends AbstractJob
{
	/**
	 * required: email, role
	 * optional: pid
	 */
	public function run($job, $params)
	{
		try {
			$gdWriteStartTime = time();

			$goodDataModel = new Model($this->appConfiguration);
			$manifests = array();
			$dataSetsToOptimize = array();
			foreach ($this->configuration->getDataSets() as $dataSet) if ($dataSet['export'] && $dataSet['isExported']) {
				$definition = $this->configuration->getDataSetDefinition($dataSet['id']);
				$manifests[] = Model::getDataLoadManifest($definition, false);
				$dataSetsToOptimize[] = Model::getDatasetId($definition['name']);
			}
			foreach ($this->configuration->getDateDimensions() as $dimension) if ($dimension['includeTime'] && $dimension['isExported']) {
				$manifests[] = json_decode($goodDataModel->getTimeDimensionDataLoadManifest($dimension['name']), true);
				$dataSetsToOptimize[] = Model::getTimeDimensionId($dimension['name']);
			}

			// Ensure that all other jobs are finished
			$this->sharedConfig->saveJob($job['id'], array('status' => SharedConfig::JOB_STATUS_WAITING));
			$i = 0;
			do {
				sleep($i * 60);
				$wait = false;
				foreach($this->sharedConfig->fetchJobs($this->configuration->projectId, $this->configuration->writerId, 2) as $job) {
					$queueIdArray = explode('.', $job['queueId']);
					if ($job['status'] == SharedConfig::JOB_STATUS_PROCESSING && (isset($queueIdArray[2]) && $queueIdArray[2] != SharedConfig::SERVICE_QUEUE)) {
						$wait = true;
					}
				}
				$i++;
			} while ($wait);
			$this->sharedConfig->saveJob($job['id'], array('status' => SharedConfig::JOB_STATUS_PROCESSING));

			$bucketAttributes = $this->configuration->bucketAttributes();
			$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

			// Check if we have all manifests
			$missing = array();
			foreach ($this->restApi->getDataSets($bucketAttributes['gd']['pid']) as $ds) {
				if (substr($ds['id'], -3) != '.dt') {
					if (!in_array($ds['id'], $dataSetsToOptimize)) {
						$missing[] = $ds['title'];
					}
				}
			}
			if (count($missing)) {
				sort($missing);
				throw new WrongConfigurationException("Following dataSets from project does not exist in writer's configuration: " . implode(', ', $missing));
			}

			$this->restApi->optimizeSliHash($bucketAttributes['gd']['pid'], $manifests);

			$this->configuration->updateWriter('maintenance', null);

			$this->logEvent('optimizeSliHash', array(
				'duration' => time() - strtotime($gdWriteStartTime)
			), $this->restApi->getLogPath());
			return array(
				'gdWriteStartTime' => $gdWriteStartTime
			);
		} catch (\Exception $e) {
			$this->configuration->updateWriter('maintenance', null);
			throw $e;
		}
	}
}