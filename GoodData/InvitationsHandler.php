<?php
/**
 * Created by PhpStorm.
 * User: JakubM
 * Date: 06.05.14
 * Time: 16:18
 */

namespace Keboola\GoodDataWriter\GoodData;

use Fetch\Server;
use Keboola\GoodDataWriter\Writer\SharedStorage;
use Monolog\Logger;

class InvitationsHandler
{
    /**
     * @var Server
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

    public function __construct(SharedStorage $sharedStorage, RestApi $restApi, Logger $logger)
    {
        $this->sharedStorage = $sharedStorage;

        $this->server = new Server('imap.gmail.com/ssl', 993);

        $this->restApi = $restApi;

        $this->logger = $logger;
    }

    public function run($emUsername, $emPassword, $gdUsername, $gdPassword)
    {
        $this->restApi->login($gdUsername, $gdPassword);
        $this->server->setAuthentication($emUsername, $emPassword);

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
                                // Do nothing
                            } else {
                                $this->restApi->post(
                                    '/gdc/account/invitations/' . $invitationId,
                                    ['invitationStatusAccept' => ['status' => 'ACCEPTED']]
                                );

                                $logData = [
                                    'pid' => substr(
                                        $result['invitation']['links']['project'],
                                        strrpos($result['invitation']['links']['project'], '/') + 1
                                    ),
                                    'sender' => $result['invitation']['meta']['author']['email'],
                                    'createDate' => $result['invitation']['meta']['created']
                                ];
                                $this->sharedStorage->logInvitation($logData);
                                $this->logger->info('Invitation processed', $logData);
                            }

                            $message->moveToMailBox('Accepted');
                        } catch (\Exception $e) {
                            $this->logger->error('Invitation failed: ' . $e->getMessage(), ['exception' => $e]);
                            $message->moveToMailBox('Failed');
                        }
                        break;
                    }
                }
            }
        }
    }
}
