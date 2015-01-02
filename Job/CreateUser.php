<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\JobProcessException;
use Keboola\GoodDataWriter\Exception\WrongParametersException;
use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\GoodData\UserAlreadyExistsException;

class CreateUser extends AbstractJob
{

	public function prepare($params)
	{
		$this->checkParams($params, array('writerId', 'firstName', 'lastName', 'email', 'password'));
		$this->checkWriterExistence($params['writerId']);
		if (strlen($params['password']) < 7) {
			throw new WrongParametersException($this->translator->trans('parameters.password_length'));
		}
		$this->configuration->checkBucketAttributes();
		$this->configuration->checkUsersTable();

		return array(
			'firstName' => $params['firstName'],
			'lastName' => $params['lastName'],
			'email' => $params['email'],
			'password' => $params['password'],
			'ssoProvider' => empty($params['ssoProvider'])? null : $params['ssoProvider']
		);
	}

	/**
	 * required: email, password, firstName, lastName
	 * optional: ssoProvider
	 */
	public function run($job, $params, RestApi $restApi)
	{
		$this->checkParams($params, array('email', 'password', 'firstName', 'lastName'));
		$params['email'] = strtolower($params['email']);

		$restApi->login($this->getDomainUser()->username, $this->getDomainUser()->password);
		$alreadyExists = false;
		try {
			$userId = $restApi->createUser($this->getDomainUser()->domain, $params['email'], $params['password'],
				$params['firstName'], $params['lastName'], $this->gdSsoProvider);
		} catch (UserAlreadyExistsException $e) {
			$userId = $e->getMessage();
			$alreadyExists = true;
			if (!$userId) {
				throw new JobProcessException($this->translator->trans('error.user.in_other_domain'));
			}
		}

		$this->configuration->saveUser($params['email'], $userId);
		if (!$alreadyExists) {
			$this->sharedStorage->saveUser($job['projectId'], $job['writerId'], $userId, $params['email']);
		}

		return array(
			'uid' => $userId,
			'alreadyExists' => $alreadyExists
		);
	}
}