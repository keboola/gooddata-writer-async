<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-07-16
 */

namespace Keboola\GoodDataWriter\Exception;

use Syrup\ComponentBundle\Exception\SyrupComponentException;

class ClientException extends SyrupComponentException
{
	public function __construct($message, $previous = null)
	{
		parent::__construct(400, $message, $previous);
	}
}