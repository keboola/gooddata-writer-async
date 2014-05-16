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
		try {
			$res = $this->restApi->get(sprintf('/gdc/md/%s', $bucketAttributes['gd']['pid']));

			$this->restApi->addUserToProject($bucketAttributes['gd']['uid'], $bucketAttributes['gd']['pid']);
			$this->configuration->updateWriter('maintenance', null);
			$this->configuration->updateWriter('waitingForInvitation', null);

		} catch (RestApiException $e) {

			if ($e->getCode() != 403) {
				throw $e;
			}

			if ($params['try'] > 5) {
				throw new WrongConfigurationException('Writer is waiting for access to your project too long. Contact support please.');
			}

			$waitJob = $this->sharedConfig->createJob($this->configuration->projectId, $this->configuration->writerId, $this->storageApiClient, array(
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
				'status' => 'error',
				'error' => 'Access to project is not granted yet.'
			);

		}

		$this->logEvent('waitForInvitation', array(), $this->restApi->getLogPath());
		return array();
	}
}