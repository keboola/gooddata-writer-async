<?php
/**
 * Created by PhpStorm.
 * User: JakubM
 * Date: 06.05.14
 * Time: 16:18
 */

namespace Keboola\GoodDataWriter\GoodData;

use Keboola\GoodDataWriter\Writer\AppConfiguration;
use Keboola\GoodDataWriter\Writer\SharedConfig;
use Symfony\Component\Process\Process;

class InvitationsHandler
{
	const SCRIPT_NAME = 'accept-invitations.rb';

	private $scriptPath;
	private $gdUsername;
	private $gdPassword;
	private $emailUsername;
	private $emailPassword;
	private $sharedConfig;

	public function __construct(AppConfiguration $appConfiguration, SharedConfig $sharedConfig)
	{
		$this->sharedConfig = $sharedConfig;
		$domainUser = $sharedConfig->getDomainUser($appConfiguration->gd_domain);
		$this->gdUsername = $domainUser->username;
		$this->gdPassword = $domainUser->password;
		$this->emailUsername = $appConfiguration->gd_invitations_email;
		$this->emailPassword = $appConfiguration->gd_invitations_password;
		$this->scriptPath = $appConfiguration->scriptsPath . '/' . self::SCRIPT_NAME;
		if (!file_exists($this->scriptPath))
			throw new \Exception('Script for accepting invitations does not exist');
	}

	public function run()
	{
		$process = new Process(sprintf('ruby %s --gd_username=%s --gd_password=%s --email_username=%s --email_password=%s',
			$this->scriptPath, escapeshellarg($this->gdUsername), escapeshellarg($this->gdPassword),
			escapeshellarg($this->emailUsername), escapeshellarg($this->emailPassword)));
		$process->setTimeout(null);
		$process->run();
		$error = $process->getErrorOutput();

		if ($process->isSuccessful() && !$error) {
			$result = $process->getOutput();
			if ($result) {
				foreach (explode("\n", $result) as $row) {
					$json = json_decode($row, true);
					if ($json && !empty($json['pid']) && !empty($json['sender']) && !empty($json['createDate']) && !empty($json['status'])) {
						$this->sharedConfig->logInvitation($json);
					}
				}
			}
		} else {
			throw new \Exception($error? $error : 'No error output');
		}
	}
} 