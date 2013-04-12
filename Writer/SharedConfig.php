<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-11
 */

namespace Keboola\GoodDataWriter\Writer;

use Keboola\StorageApi\Client as StorageApiClient,
	\Keboola\StorageApi\Table as StorageApiTable;

class SharedConfig
{
	const JOBS_TABLE_ID = 'in.c-wr-gooddata.jobs';
	const PROJECTS_TABLE_ID = 'in.c-wr-gooddata.projects';
	const USERS_TABLE_ID = 'in.c-wr-gooddata.users';
	const PROJECTS_TO_DELETE_TABLE_ID = 'in.c-wr-gooddata.projects_to_delete';
	const USERS_TO_DELETE_TABLE_ID = 'in.c-wr-gooddata.users_to_delete';


	/**
	 * @var StorageApiClient
	 */
	private $_storageApiClient;

	public function __construct($storageApiClient)
	{
		$this->_storageApiClient = $storageApiClient;
	}



	/**
	 * @param $jobId
	 * @return mixed
	 */
	public function fetchJob($jobId)
	{
		$csv = $this->_storageApiClient->exportTable(
			self::JOBS_TABLE_ID,
			null,
			array(
				'whereColumn' => 'id',
				'whereValues' => array($jobId),
			)
		);

		$jobs = StorageApiClient::parseCsv($csv, true);
		return reset($jobs);
	}

	/**
	 * @param $jobId
	 * @param $fields
	 */
	public function saveJob($jobId, $fields)
	{
		$jobsTable = new StorageApiTable($this->_storageApiClient, self::JOBS_TABLE_ID);
		$jobsTable->setHeader(array_merge(array('id'), array_keys($fields)));
		$jobsTable->setFromArray(array(array_merge(array($jobId), $fields)));
		$jobsTable->setPartial(true);
		$jobsTable->setIncremental(true);
		$jobsTable->save();
	}



	public function saveProject($pid, $accessToken, $backendUrl, $job)
	{
		$data = array(
			'pid' => $pid,
			'projectId' => $job['projectId'],
			'writerId' => $job['writerId'],
			'backendUrl' => $backendUrl,
			'accessToken' => $accessToken,
			'createdTime' => date('c')
		);
		$table = new StorageApiTable($this->_storageApiClient, self::PROJECTS_TABLE_ID);
		$table->setHeader(array_keys($data));
		$table->setFromArray(array($data));
		$table->setIncremental(true);
		$table->save();
	}

	public function saveUser($uri, $email, $job)
	{
		$data = array(
			'uri' => $uri,
			'projectId' => $job['projectId'],
			'writerId' => $job['writerId'],
			'email' => $email,
			'createdTime' => date('c')
		);
		$table = new StorageApiTable($this->_storageApiClient, self::USERS_TABLE_ID);
		$table->setHeader(array_keys($data));
		$table->setFromArray(array($data));
		$table->setIncremental(true);
		$table->save();
	}



	/**
	 * @param $projectId
	 * @param $writerId
	 * @param $pid
	 * @param int $dev
	 */
	public function enqueueProjectToDelete($projectId, $writerId, $pid, $dev = 0)
	{
		$data = array(
			'pid' => $pid,
			'projectId' => $projectId,
			'writerId' => $writerId,
			'deleteDate' => date('c', strtotime('+30 days')),
			'dev' => $dev
		);
		$table = new StorageApiTable($this->_storageApiClient, self::PROJECTS_TO_DELETE_TABLE_ID, null, 'pid');
		$table->setHeader(array_keys($data));
		$table->setFromArray(array($data));
		$table->setPartial(true);
		$table->setIncremental(true);
		$table->save();
	}

	/**
	 * @param $projectId
	 * @param $writerId
	 * @param $uri
	 * @param $email
	 * @param int $dev
	 */
	public function enqueueUserToDelete($projectId, $writerId, $uri, $email, $dev = 0)
	{
		$data = array(
			'uri' => $uri,
			'projectId' => $projectId,
			'writerId' => $writerId,
			'email' => $email,
			'deleteDate' => date('c', strtotime('+30 days')),
			'dev' => $dev
		);
		$table = new StorageApiTable($this->_storageApiClient, self::USERS_TO_DELETE_TABLE_ID, null, 'uri');
		$table->setHeader(array_keys($data));
		$table->setFromArray(array($data));
		$table->setPartial(true);
		$table->setIncremental(true);
		$table->save();
	}
}