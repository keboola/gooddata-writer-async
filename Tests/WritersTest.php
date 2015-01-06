<?php
/**
 * @package gooddata-writer
 * @copyright 2015 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\GoodDataWriter\Tests;


use Keboola\GoodDataWriter\Writer\Configuration;
use Keboola\StorageApi\Client;

class WritersTest extends AbstractFunctionalTest
{

	public function testWriters()
	{
		$storageApiClient = new Client(array('token' => STORAGE_API_TOKEN));
		$configuration = new Configuration($storageApiClient, $this->sharedStorage);

		//@TODO připravit konfiguraci

		$job = $this->getJob('createWriter', $configuration, $storageApiClient);

		$writerId = uniqid();
		$inputParams = array('writerId' => $writerId);
		$checkedParams = $job->prepare($inputParams, $this->restApi);

		$jobInfo = array(
			'id' => uniqid(),
			'batchId' => uniqid(),
			'projectId' => rand(1, 128),
			'writerId' => $writerId,
			'token' => STORAGE_API_TOKEN,
			'tokenId' => rand(1, 128),
			'tokenDesc' => uniqid(),
			'createdTime' => date('c'),
			'command' => 'createWriter',
			'parameters' => $checkedParams
		);
		$job->run($jobInfo, $checkedParams, $this->restApi);

	}

}
