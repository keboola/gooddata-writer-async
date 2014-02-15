<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-05-07
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;

class DeleteDataSet extends AbstractJob
{
	/**
	 * @param $job
	 * @param $params
	 * @throws WrongConfigurationException
	 * @return array
	 */
	public function run($job, $params)
	{
		$this->checkParams($params, array('dataset', 'tableId'));

		$this->configuration->checkBucketAttributes();

		$projects = $this->configuration->getProjects();
		$gdWriteStartTime = date('c');

		$bucketAttributes = $this->configuration->bucketAttributes();
		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
		$this->restApi->getProject($bucketAttributes['gd']['pid']);

		foreach ($projects as $project) if ($project['active']) {
			$this->restApi->dropDataset($project['pid'], $job['dataset']);
		}

		$this->configuration->updateDataSetDefinition($params['tableId'], 'lastExportDate', '');
		$this->logEvent('deleteDataSet', array(
			'duration' => time() - strtotime($gdWriteStartTime)
		), $this->restApi->getLogPath());
		return array(
			'gdWriteStartTime' => $gdWriteStartTime
		);
	}
}