<?php
/**
 * Created by PhpStorm.
 * User: JakubM
 * Date: 06.05.14
 * Time: 16:18
 */

namespace Keboola\GoodDataWriter\GoodData;

use Keboola\GoodDataWriter\Writer\SharedStorage;
use Monolog\Logger;
use Symfony\Component\Process\Process;

class InvitationsHandler
{
    /**
     * @var \Fetch\Server
     */
    private $server;
    /**
     * @var SharedStorage
     */
    private $sharedStorage;
    /**
     * @var RestApi
     */
    private $restApi;
    /**
     * @var Logger
     */
    private $logger;

    private $gdUsername;
    private $gdPassword;
    private $emailUsername;
    private $emailPassword;

    public function __construct(array $config, SharedStorage $sharedStorage, RestApi $restApi, Logger $logger)
    {
        if (!isset($config['domain'])) {
            throw new \Exception("Key 'domain' is missing from invitations config");
        }
        if (!isset($config['email'])) {
            throw new \Exception("Key 'email' is missing from invitations config");
        }
        if (!isset($config['password'])) {
            throw new \Exception("Key 'password' is missing from invitations config");
        }

        $this->sharedStorage = $sharedStorage;
        $domainUser = $sharedStorage->getDomainUser($config['domain']);
        $this->gdUsername = $domainUser->username;
        $this->gdPassword = $domainUser->password;
        $this->emailUsername = $config['email'];
        $this->emailPassword = $config['password'];

        $this->server = new \Fetch\Server('imap.gmail.com/ssl', 993);
        $this->server->setAuthentication($config['email'], $config['password']);

        $domainUser = $this->sharedStorage->getDomainUser($config['domain']);

        $this->restApi = $restApi;
        $restApi->login($domainUser->username, $domainUser->password);

        $this->logger = $logger;
    }

    public function run()
    {
        $messages = $this->server->getMessages();
        /** @var $message \Fetch\Message */
        foreach ($messages as $message) {
            $sender = current($message->getHeaders()->from);
            if ($sender->mailbox == 'invitation' && $sender->host == 'gooddata.com') {
                $body = $message->getMessageBody();
                foreach (explode("\n", $body) as $row) {
                    if (strpos($row, 'https://secure.gooddata.com') === 0) {
                        try {
                            $invitationId = substr($row, strrpos($row, '/') + 1);
                            $result = $this->restApi->get('/gdc/account/invitations/' . $invitationId);
                            if (!isset($result['invitation']['content']['status'])) {
                                throw new \Exception('ERROR');
                            }
                            if ($result['invitation']['content']['status'] == 'ACCEPTED') {
                                return true;
                            }

                            $this->restApi->post(
                                '/gdc/account/invitations/' . $invitationId,
                                ['invitationStatusAccept' => ['status' => 'ACCEPTED']]
                            );

                            $this->sharedStorage->logInvitation([
                                'pid' => substr(
                                    $result['invitation']['links']['project'],
                                    strrpos($result['invitation']['links']['project'], '/') + 1
                                ),
                                'sender' => $result['invitation']['meta']['author']['email'],
                                'createDate' => $result['invitation']['meta']['created']
                            ]);

                            $message->moveToMailBox('Accepted');
                        } catch (\Exception $e) {
                            $this->logger->alert('Invitation failed: ' . $e->getMessage(), ['exception' => $e]);
                            $message->moveToMailBox('Failed');
                        }
                        break;
                    }
                }
            }
        }
    }
}
