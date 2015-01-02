<?php
/**
 * Created by PhpStorm.
 * User: JakubM
 * Date: 06.05.14
 * Time: 16:18
 */

namespace Keboola\GoodDataWriter\GoodData;

use Keboola\GoodDataWriter\Writer\SharedStorage;
use Symfony\Component\Process\Process;

class InvitationsHandler
{
	const SCRIPT_NAME = 'accept-invitations.rb';

	private $scriptPath;
	private $gdUsername;
	private $gdPassword;
	private $emailUsername;
	private $emailPassword;
	private $sharedStorage;

	public function __construct($scriptsPath, $config, SharedStorage $sharedStorage)
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
		$this->scriptPath = $scriptsPath . '/' . self::SCRIPT_NAME;
		if (!file_exists($this->scriptPath))
			throw new \Exception('Script for accepting invitations does not exist');
		if (isset($config['ruby_path'])) {
			$this->rubyPath = $config['ruby_path'];
			if (!file_exists($this->rubyPath))
				throw new \Exception('Ruby on path defined in parameters.yml does not exist');
		} else {
			$this->rubyPath = 'ruby';
		}
	}

	public function run()
	{
		$process = new Process(sprintf('%s %s --gd_username=%s --gd_password=%s --email_username=%s --email_password=%s',
			$this->rubyPath, $this->scriptPath, escapeshellarg($this->gdUsername), escapeshellarg($this->gdPassword),
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
						$this->sharedStorage->logInvitation($json);
					}
				}
			}
		} else {
			throw new \Exception($error? $error : 'No error output');
		}
	}
} 