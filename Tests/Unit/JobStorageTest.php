<?php
/**
 * @package gooddata-writer
 * @copyright 2014 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\GoodDataWriter\Tests\Unit;

use Doctrine\DBAL\Connection;
use Keboola\GoodDataWriter\Writer\JobStorage;

class JobStorageTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var JobStorage $jobStorage
     */
    private $jobStorage;
    /**
     * @var Connection $db
     */
    private $db;

    public function setUp()
    {
        $this->db = \Doctrine\DBAL\DriverManager::getConnection([
            'driver' => 'pdo_mysql',
            'host' => DB_HOST,
            'dbname' => DB_NAME,
            'user' => DB_USER,
            'password' => DB_PASSWORD,
        ]);

        $this->jobStorage = new JobStorage($this->db);
    }

    /**
     * @throws \Keboola\GoodDataWriter\Exception\SharedStorageException
     */
    public function testJobs()
    {
        $projectId = rand(1, 255);
        $writerId = uniqid();
        $jobId = rand(1, 1024);
        $command = uniqid();

        // fetchJobs()
        $jobs = $this->jobStorage->fetchJobs($projectId, $writerId);
        $this->assertCount(0, $jobs);

        // createJob()
        $this->jobStorage->createJob($jobId, $projectId, $writerId, [
            'command' => $command,
            'token' => uniqid(),
            'tokenId' => rand(1, 128),
            'tokenDesc' => uniqid(),
            'createdTime' => date('c')
        ]);

        // fetchJob(), fetchJobs()
        $jobs = $this->jobStorage->fetchJobs($projectId, $writerId);
        $this->assertCount(1, $jobs);
        $job = $this->jobStorage->fetchJob($jobId, $projectId, $writerId);
        $this->assertArrayHasKey('command', $job);
        $this->assertEquals($command, $job['command']);

        // saveJob()
        $command = uniqid();
        $this->jobStorage->saveJob($jobId, ['command' => $command]);
        $job = $this->jobStorage->fetchJob($jobId);
        $this->assertArrayHasKey('command', $job);
        $this->assertEquals($command, $job['command']);

        // isJobFinished(), cancelJobs()
        $this->assertFalse(JobStorage::isJobFinished($job['status']));
        $this->jobStorage->cancelJobs($projectId, $writerId);
        $job = $this->jobStorage->fetchJob($jobId);
        $this->assertTrue(JobStorage::isJobFinished($job['status']));

        // fetchBatch()
        $batch = $this->jobStorage->fetchBatch($jobId);
        $this->assertCount(1, $batch);

        // jobToApiResponse()
        $job = $this->jobStorage->fetchJob($jobId);
        $response = JobStorage::jobToApiResponse($job);
        $this->assertArrayHasKey('token', $response);
        $this->assertArrayHasKey('id', $response['token']);
        $this->assertArrayHasKey('description', $response['token']);
        $this->assertArrayHasKey('logs', $response);

        // batchToApiResponse()
        $response = JobStorage::batchToApiResponse($jobId, [$job]);
        $this->assertArrayHasKey('jobs', $response);
        $this->assertCount(1, $response['jobs']);
    }
}
