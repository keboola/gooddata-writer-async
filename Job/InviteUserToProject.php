<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\GoodDataWriter\GoodData\RestApi;

class InviteUserToProject extends AbstractJob
{
	/**
	 * required: email, role
	 * optional: pid
	 */
	public function run($job, $params)
	{
		$this->checkParams($params, array('email', 'role'));
		$params['email'] = strtolower($params['email']);

		$allowedRoles = array_keys(RestApi::$userRoles);
		if (!in_array($params['role'], $allowedRoles)) {
			throw new WrongConfigurationException("Parameter 'role' is not valid; it has to be one of: " . implode(', ', $allowedRoles));
		}
		$this->configuration->checkBucketAttributes();
		$bucketAttributes = $this->configuration->bucketAttributes();

		if (empty($params['pid'])) {
			if (empty($bucketAttributes['gd']['pid'])) {
				throw new WrongConfigurationException("Parameter 'pid' is missing and writer does not have primary project");
			}
			$params['pid'] = $bucketAttributes['gd']['pid'];
		}


		$gdWriteStartTime = date('c');
		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
		$this->restApi->inviteUserToProject($params['email'], $params['pid'], RestApi::$userRoles[$params['role']]);

		$this->configuration->saveProjectUser($params['pid'], $params['email'], $params['role']);

		$this->logEvent('inviteUserToProject', array(
			'duration' => time() - strtotime($gdWriteStartTime)
		), $this->restApi->getLogPath());
		return array(
			'gdWriteStartTime' => $gdWriteStartTime
		);
	}
}