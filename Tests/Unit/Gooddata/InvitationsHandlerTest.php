<?php
/**
 * @package gooddata-writer
 * @copyright 2015 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */
namespace Unit\Gooddata;

use Keboola\GoodDataWriter\GoodData\InvitationsHandler;
use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\Writer\SharedStorage;
use Keboola\Syrup\Encryption\Encryptor;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;

class InvitationsHandlerTest extends \PHPUnit_Framework_TestCase
{
    /** @var InvitationsHandler */
    protected $invitationsHandler;
    /** @var RestApi */
    protected $restApi;

    const PROJECT_NAME_PREFIX = '[test-invitation] ';

    protected function setUp()
    {
        $db = \Doctrine\DBAL\DriverManager::getConnection([
            'driver' => 'pdo_mysql',
            'host' => GW_DB_HOST,
            'dbname' => GW_DB_NAME,
            'user' => GW_DB_USER,
            'password' => GW_DB_PASSWORD
        ]);
        $encryptor = new Encryptor(GW_ENCRYPTION_KEY);
        $sharedStorage = new SharedStorage($db, $encryptor);
        $logger = new \Monolog\Logger(GW_APP_NAME);
        $logger->pushHandler(new StreamHandler(STDERR, Logger::ERROR));
        $this->restApi = new RestApi(GW_APP_NAME, $logger);

        $this->invitationsHandler = new InvitationsHandler($sharedStorage, $this->restApi, $logger);

        // cleanup
        $this->restApi->login(GW_GD_DOMAIN_USER, GW_GD_DOMAIN_PASSWORD);
        foreach ($this->restApi->get('/gdc/account/profile/'.GW_GD_DOMAIN_UID.'/projects')['projects'] as $p) {
            if (strpos($p['project']['meta']['title'], self::PROJECT_NAME_PREFIX) !== false) {
                //$this->restApi->dropProject(substr($p['project']['links']['self'], strrpos($p['project']['links']['self'], '/')+1));
            }
        }
    }

    public function testInvitations()
    {
        return false;

        //@TODO does not work

        $this->restApi->login(GW_GD_DOMAIN_USER, GW_GD_DOMAIN_PASSWORD);

        $pid = $this->restApi->createProject(self::PROJECT_NAME_PREFIX.uniqid(), GW_GD_ACCESS_TOKEN);
        $this->restApi->inviteUserToProject(GW_INVITATION_EMAIL, $pid);

        $server = new \Fetch\Server('imap.gmail.com/ssl', 993);
        $server->setAuthentication(GW_INVITATION_EMAIL, GW_INVITATION_PASSWORD);
        /** @var $message \Fetch\Message */
        $messagesCount = count($server->getMessages());

        $startTime = time();
        do {
            sleep(10);
        } while ($messagesCount < count($server->getMessages()) || (time() - $startTime) < 300);

        $this->invitationsHandler->run(
            GW_INVITATION_EMAIL,
            GW_INVITATION_PASSWORD,
            GW_INVITATION_EMAIL,
            GW_INVITATION_PASSWORD
        );

        $this->restApi->login(GW_GD_DOMAIN_USER, GW_GD_DOMAIN_PASSWORD);
        $users = $this->restApi->usersInProject($pid);
        print_r($users);
    }

}
