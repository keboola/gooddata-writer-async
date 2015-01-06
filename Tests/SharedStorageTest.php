<?php
/**
 * @package gooddata-writer
 * @copyright 2014 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\GoodDataWriter\Tests;

use Doctrine\DBAL\Connection;
use Keboola\GoodDataWriter\Writer\SharedStorage;
use Syrup\ComponentBundle\Encryption\Encryptor;

class SharedStorageTest extends \PHPUnit_Framework_TestCase
{
	/**
	 * @var SharedStorage $sharedStorage
	 */
	private $sharedStorage;
	/**
	 * @var Connection $db
	 */
	private $db;

	public function setUp()
	{
		$this->db = \Doctrine\DBAL\DriverManager::getConnection(array(
			'driver' => 'pdo_mysql',
			'host' => DB_HOST,
			'dbname' => DB_NAME,
			'user' => DB_USER,
			'password' => DB_PASSWORD,
		));

		$this->sharedStorage = new SharedStorage($this->db, new Encryptor(ENCRYPTION_KEY));
	}

	public function testWriters()
	{
		$projectId = rand(1, 255);
		$writerId = uniqid();

		// createWriter()
		$this->sharedStorage->createWriter($projectId, $writerId, 'bucket', 255, 'tokenDescription');

		// writerExists()
		$this->assertTrue($this->sharedStorage->writerExists($projectId, $writerId));

		// getWriter()
		$writer = $this->sharedStorage->getWriter($projectId, $writerId);
		$this->assertArrayHasKey('status', $writer);
		$this->assertArrayHasKey('bucket', $writer);
		$this->assertArrayHasKey('info', $writer);
		$this->assertArrayHasKey('created', $writer);
		$this->assertArrayHasKey('time', $writer['created']);
		$this->assertArrayHasKey('tokenId', $writer['created']);
		$this->assertArrayHasKey('tokenDescription', $writer['created']);

		// setWriterStatus()
		$this->sharedStorage->setWriterStatus($projectId, $writerId, SharedStorage::WRITER_STATUS_MAINTENANCE);
		$result = $this->db->fetchAll('SELECT * FROM writers WHERE project_id = ? AND writer_id=?', array($projectId, $writerId));
		$this->assertCount(1, $result);
		$writer = current($result);
		$this->assertArrayHasKey('status', $writer);
		$this->assertEquals(SharedStorage::WRITER_STATUS_MAINTENANCE, $writer['status']);

		// updateWriter()
		$testValue = uniqid();
		$this->sharedStorage->updateWriter($projectId, $writerId, array('token_desc' => $testValue));
		$result = $this->db->fetchAll('SELECT * FROM writers WHERE project_id = ? AND writer_id=?', array($projectId, $writerId));
		$this->assertCount(1, $result);
		$writer = current($result);
		$this->assertArrayHasKey('token_desc', $writer);
		$this->assertEquals($testValue, $writer['token_desc']);

		// deleteWriter()
		$this->sharedStorage->deleteWriter($projectId, $writerId);
		$result = $this->db->fetchAll('SELECT * FROM writers WHERE project_id = ? AND writer_id=?', array($projectId, $writerId));
		$this->assertCount(1, $result);
		$writer = current($result);
		$this->assertArrayHasKey('status', $writer);
		$this->assertEquals(SharedStorage::WRITER_STATUS_DELETED, $writer['status']);
	}

	public function testProjects()
	{
		$projectId = rand(1, 255);
		$writerId = uniqid();
		$pid = md5($writerId);

		// saveProject()
		$this->sharedStorage->saveProject($projectId, $writerId, $pid);

		// getProjects()
		$projects = $this->sharedStorage->getProjects();
		$this->assertCount(1, $projects);
		$projects = $this->sharedStorage->getProjects($projectId, $writerId);
		$this->assertCount(1, $projects);

		// projectBelongsToWriter()
		$this->assertTrue($this->sharedStorage->projectBelongsToWriter($projectId, $writerId, $pid));

		// projectsToDelete(), enqueueProjectToDelete()
		$this->assertCount(0, $this->sharedStorage->projectsToDelete());
		$this->sharedStorage->enqueueProjectToDelete($projectId, $writerId, $pid);
		$r = $this->db->fetchAll('SELECT * FROM projects WHERE project_id=? AND writer_id=?', array($projectId, $writerId));
		$this->assertCount(1, $r);
		$r = current($r);
		$this->assertNotEmpty($r['removal_time']);
		$this->assertEmpty($r['deleted_time']);

		// markProjectsDeleted()
		$this->sharedStorage->markProjectsDeleted(array($pid));
		$r = $this->db->fetchAll('SELECT * FROM projects WHERE project_id=? AND writer_id=?', array($projectId, $writerId));
		$this->assertCount(1, $r);
		$r = current($r);
		$this->assertNotEmpty($r['deleted_time']);
	}

	public function testUsers()
	{
		$projectId = rand(1, 255);
		$writerId = uniqid();
		$uid = md5($writerId);
		$email = uniqid();

		// saveUser()
		$this->sharedStorage->saveUser($projectId, $writerId, $uid, $email);

		// getUsers()
		$users = $this->sharedStorage->getUsers($projectId, $writerId);
		$this->assertCount(1, $users);

		// userBelongsToWriter()
		$this->assertTrue($this->sharedStorage->userBelongsToWriter($projectId, $writerId, $email));

		// usersToDelete(), enqueueUserToDelete()
		$this->assertCount(0, $this->sharedStorage->usersToDelete());
		$this->sharedStorage->enqueueUserToDelete($projectId, $writerId, $uid);
		$r = $this->db->fetchAll('SELECT * FROM users WHERE project_id=? AND writer_id=?', array($projectId, $writerId));
		$this->assertCount(1, $r);
		$r = current($r);
		$this->assertNotEmpty($r['removal_time']);
		$this->assertEmpty($r['deleted_time']);

		// markUsersDeleted()
		$this->sharedStorage->markUsersDeleted(array($uid));
		$r = $this->db->fetchAll('SELECT * FROM users WHERE project_id=? AND writer_id=?', array($projectId, $writerId));
		$this->assertCount(1, $r);
		$r = current($r);
		$this->assertNotEmpty($r['deleted_time']);
	}

	public function testJobs()
	{
		$projectId = rand(1, 255);
		$writerId = uniqid();
		$jobId = rand(1, 1024);
		$command = uniqid();

		// fetchJobs()
		$jobs = $this->sharedStorage->fetchJobs($projectId, $writerId);
		$this->assertCount(0, $jobs);

		// createJob()
		$this->sharedStorage->createJob($jobId, $projectId, $writerId, array('command' => $command, 'token' => uniqid(),
			'tokenId' => rand(1, 128), 'tokenDesc' => uniqid(), 'createdTime' => date('c')));

		// fetchJob(), fetchJobs()
		$jobs = $this->sharedStorage->fetchJobs($projectId, $writerId);
		$this->assertCount(1, $jobs);
		$job = $this->sharedStorage->fetchJob($jobId, $projectId, $writerId);
		$this->assertArrayHasKey('command', $job);
		$this->assertEquals($command, $job['command']);

		// saveJob()
		$command = uniqid();
		$this->sharedStorage->saveJob($jobId, array('command' => $command));
		$job = $this->sharedStorage->fetchJob($jobId);
		$this->assertArrayHasKey('command', $job);
		$this->assertEquals($command, $job['command']);

		// isJobFinished(), cancelJobs()
		$this->assertFalse($this->sharedStorage->isJobFinished($job['status']));
		$this->sharedStorage->cancelJobs($projectId, $writerId);
		$job = $this->sharedStorage->fetchJob($jobId);
		$this->assertTrue($this->sharedStorage->isJobFinished($job['status']));

		// fetchBatch()
		$batch = $this->sharedStorage->fetchBatch($jobId);
		$this->assertCount(1, $batch);

		// jobToApiResponse()
		$job = $this->sharedStorage->fetchJob($jobId);
		$response = SharedStorage::jobToApiResponse($job);
		$this->assertArrayHasKey('token', $response);
		$this->assertArrayHasKey('id', $response['token']);
		$this->assertArrayHasKey('description', $response['token']);
		$this->assertArrayHasKey('logs', $response);

		// batchToApiResponse()
		$response = SharedStorage::batchToApiResponse($jobId, array($job));
		$this->assertArrayHasKey('jobs', $response);
		$this->assertCount(1, $response['jobs']);
	}

}
