<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2014-05-14
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\GoodDataWriter\GoodData\RestApiException;
use Keboola\GoodDataWriter\Writer\SharedConfig;

class WaitForInvitation extends AbstractJob
{
	/**
	 * required:
	 * optional:
	 */
	public function run($job, $params)
	{
		$this->checkParams($params, array('try'));
		$bucketAttributes = $this->configuration->bucketAttributes();

		$this->restApi->login($this->domainUser->username, $this->domainUser->password);
		if ($this->restApi->hasAccessToProject($bucketAttributes['gd']['pid'])) {

			$this->restApi->addUserToProject($bucketAttributes['gd']['uid'], $bucketAttributes['gd']['pid']);
			$this->configuration->updateWriter('maintenance', null);
			$this->configuration->updateWriter('waitingForInvitation', null);

		} else {

			if ($params['try'] > 5) {
				throw new WrongConfigurationException('Writer is waiting for access to your project too long. Contact support please.');
			}

			$tokenData = $this->storageApiClient->getLogData();
			$waitJob = $this->sharedConfig->createJob($this->configuration->projectId, $this->configuration->writerId,
				$this->storageApiClient->getRunId(), $this->storageApiClient->token, $tokenData['id'], $tokenData['description'], array(
				'command' => 'waitForInvitation',
				'createdTime' => date('c'),
				'parameters' => array(
					'try' => $params['try'] + 1
				),
				'queue' => SharedConfig::SERVICE_QUEUE
			));
			$this->queue->enqueue(array(
				'projectId' => $this->configuration->projectId,
				'writerId' => $this->configuration->writerId,
				'batchId' => $waitJob['batchId']
			), $params['try'] * 60);

			return array(
				'status' => SharedConfig::JOB_STATUS_ERROR,
				'error' => 'Access to project is not granted yet'
			);

		}

		$this->logEvent('waitForInvitation', array(), $this->restApi->getLogPath());
		return array();
	}
}