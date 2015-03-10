<?php
/**
 * @package gooddata-writer
 * @copyright 2014 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\GoodDataWriter\Tests\Unit\Writer;

use Doctrine\DBAL\Connection;
use Keboola\GoodDataWriter\Writer\SharedStorage;
use Keboola\Syrup\Encryption\Encryptor;

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
        $this->db = \Doctrine\DBAL\DriverManager::getConnection([
            'driver' => 'pdo_mysql',
            'host' => GW_DB_HOST,
            'dbname' => GW_DB_NAME,
            'user' => GW_DB_USER,
            'password' => GW_DB_PASSWORD,
        ]);
        $this->db->executeQuery('TRUNCATE TABLE projects');
        $this->db->executeQuery('TRUNCATE TABLE writers');
        $this->db->executeQuery('TRUNCATE TABLE users');

        $this->sharedStorage = new SharedStorage($this->db, new Encryptor(GW_ENCRYPTION_KEY));
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
        $result = $this->db->fetchAll(
            'SELECT * FROM writers WHERE project_id = ? AND writer_id=?',
            [$projectId, $writerId]
        );
        $this->assertCount(1, $result);
        $writer = current($result);
        $this->assertArrayHasKey('status', $writer);
        $this->assertEquals(SharedStorage::WRITER_STATUS_MAINTENANCE, $writer['status']);

        // updateWriter()
        $testValue = uniqid();
        $this->sharedStorage->updateWriter($projectId, $writerId, ['token_desc' => $testValue]);
        $result = $this->db->fetchAll(
            'SELECT * FROM writers WHERE project_id = ? AND writer_id=?',
            [$projectId, $writerId]
        );
        $this->assertCount(1, $result);
        $writer = current($result);
        $this->assertArrayHasKey('token_desc', $writer);
        $this->assertEquals($testValue, $writer['token_desc']);

        // deleteWriter()
        $this->sharedStorage->deleteWriter($projectId, $writerId);
        $result = $this->db->fetchAll(
            'SELECT * FROM writers WHERE project_id = ? AND writer_id=?',
            [$projectId, $writerId]
        );
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
        $r = $this->db->fetchAll('SELECT * FROM projects WHERE project_id=? AND writer_id=?', [$projectId, $writerId]);
        $this->assertCount(1, $r);
        $r = current($r);
        $this->assertNotEmpty($r['removal_time']);
        $this->assertEmpty($r['deleted_time']);

        // markProjectsDeleted()
        $this->sharedStorage->markProjectsDeleted([$pid]);
        $r = $this->db->fetchAll('SELECT * FROM projects WHERE project_id=? AND writer_id=?', [$projectId, $writerId]);
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
        $r = $this->db->fetchAll('SELECT * FROM users WHERE project_id=? AND writer_id=?', [$projectId, $writerId]);
        $this->assertCount(1, $r);
        $r = current($r);
        $this->assertNotEmpty($r['removal_time']);
        $this->assertEmpty($r['deleted_time']);

        // markUsersDeleted()
        $this->sharedStorage->markUsersDeleted([$uid]);
        $r = $this->db->fetchAll('SELECT * FROM users WHERE project_id=? AND writer_id=?', [$projectId, $writerId]);
        $this->assertCount(1, $r);
        $r = current($r);
        $this->assertNotEmpty($r['deleted_time']);
    }
}
