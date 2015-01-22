<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2014-05-14
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\Writer\SharedStorage;

class WaitForInvitation extends AbstractJob
{

	public function prepare($params)
	{

	}

	/**
	 * required:
	 * optional:
	 */
	public function run($job, $params, RestApi $restApi)
	{
		$this->checkParams($params, array('try'));
		$bucketAttributes = $this->configuration->bucketAttributes();

		$restApi->login($this->getDomainUser()->username, $this->getDomainUser()->password);
		if ($restApi->hasAccessToProject($bucketAttributes['gd']['pid'])) {

			$restApi->addUserToProject($bucketAttributes['gd']['uid'], $bucketAttributes['gd']['pid']);
			$this->sharedStorage->setWriterStatus($job['projectId'], $job['writerId'], SharedStorage::WRITER_STATUS_READY);
			$this->configuration->updateBucketAttribute('waitingForInvitation', null);

		} else {

			if ($params['try'] > 5) {
				throw new WrongConfigurationException($this->translator->trans('wait_for_invitation.lasts_too_long'));
			}

			$waitJobData = $this->factory->createJob('waitForInvitation', array('try' => $params['try'] + 1), null, SharedStorage::SERVICE_QUEUE);
			$this->factory->enqueueJob($waitJobData['batchId'], $params['try'] * 60);

			return array(
				'status' => SharedStorage::JOB_STATUS_ERROR,
				'error' => $this->translator->trans('wait_for_invitation.not_yet_ready')
			);

		}

		return array();
	}
}