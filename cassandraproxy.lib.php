<?php

namespace RSquare\Cassandra;

class Proxy {
	var $connection;
	var $socket;
	var $host;
	var $port;
	var $keyspace;
	var $queryData;
	var $recordData;
	var $timeout;
	var $logging;
	var $error;

	/**
	 * Constructor
	 * @param string host proxyhost
	 * @param string port proxyport
	 * @param string keyspace optional if provided the cassandra is automaticaly set to use the keyspace
	 **/
	public function __construct($host,$port,$keyspace='')
	{
		$this->connection=null;
		$this->socket=null;
		$this->host=$host;
		$this->port=$port;
		$this->keyspace=$keyspace;
		$this->queryData;
		$this->recordData;
		$this->timeout=10;//default to 10 sec timeout.
		$this->error=null;
	}

	/**
	 * destructor
	 **/
	function __destruct() {
		// close the connection session and perform the cleanup.
		if(isset($this->socket)) socket_close ($this->socket);
	}

	/**
	 * isConnected
	 * returns if the connection is active or not, you can force a connection by parameter.
	 * @param int forceConnect default false
	 * if not connected yet, connects to the Cassandra proxy
	 * @return int 0=Not connected; 1= Connected;
	 **/
	public function isConnected($forceConnection=0){
		if($this->connection){
			return 1;
		}
		if($forceConnection){
			$result=$this->connectProxy();
			if($result['success']==false) return 0;
			else return 1;
		}
		return 0;
	}


	/**
	 * connectProxy
	 * if not connected yet, connects to the Cassandra proxy
	 * @return int 0=ok; 1=connection error; 2=keyspace error;
	 **/
	public function connectProxy(){
		if($this->connection==null){
			$context = stream_context_create();
			$ssl=0;
			//$target=sprintf('%s://%s:%d', ($ssl === true ? 'ssl' : 'tcp'), $this->host, $this->port);
			$target=sprintf('tcp://%s:%d', $this->host, $this->port);
			if ($this->connection = stream_socket_client($target, $errno, $errstr, $this->timeout, STREAM_CLIENT_CONNECT, $context)){
				$this->writeLog("Connection made");
				stream_set_timeout($this->connection,$this->timeout);
				stream_set_blocking($this->connection,true);
			}else{
				echo "socket_connect() failed: reason: " . socket_strerror(socket_last_error($this->connection)) . "\n";
				return 1;
			}
			//var_dump($this->socket); print $this->host. $this->port;
			if($this->connection==true){
				if(!empty($this->keyspace)){
					$cql="use ".$this->keyspace.";";
					$result=$this->executeCQL($cql);
					if($result['success']==false) return 2;
				}
			}
		}
		return 0;
	}

	/**
	 * This will read 1 response from the connection
	 * @return string
	 */
	public function read(){
		putenv('SURPRESS_ERROR_HANDLER=1');
		$content = '';
		$time = time()+$this->timeout;

		while ((!isset ($length)) || ($length > 0)){
			if (feof($this->connection)){   
				putenv('SURPRESS_ERROR_HANDLER=0');
				return false;
			}
			//Check if timeout occured
			if (time() >= $time){   
				putenv('SURPRESS_ERROR_HANDLER=0');
				return false;
			}
			//If we dont know how much to read we read the first few bytes first, these contain the content-lenght
			//of whats to come
			if ((!isset($length)) || ($length == 0)){
				$readLength = 4;
				$readbuffer = "";
				$read = "";
				while ($readLength > 0){  
					if ($readbuffer = fread($this->connection, $readLength)){
						$readLength = $readLength - strlen($readbuffer);
						$read .= $readbuffer;
					}
					//Check if timeout occured
					if (time() >= $time){  
						putenv('SURPRESS_ERROR_HANDLER=0');
						return false;
					}
				}
				$this->writeLog("Reading 4 bytes for integer. (read: ".strlen($read).")");
				$this->writeLog($read);
				$length = $this->readInteger($read)-4;
				$this->writeLog("Reading next: $length bytes");
			}
 
			//We know the length of what to read, so lets read the stuff
			if ((isset($length)) && ($length > 0)){
				$time = time()+$this->timeout;
				$this->writeLog("Reading $length bytes of content.");
				if ($read = fread($this->connection, $length)){
					$this->writeLog($read);
					$this->writeLog(print_r(socket_get_status($this->connection), true));
					$length = $length-strlen($read);
					$content .= $read;
				}
			}
			if (!strlen($read)){
				usleep(100);
			}
		}
		putenv('SURPRESS_ERROR_HANDLER=0');
		//echo "\n".$length." ".$content; ob_flush();
		return $content;
	}

	/**
	 * This parses the first 4 bytes into an integer for use to compare content-length
	 *
	 * @param string $content
	 * @return integer
	 */
	private function readInteger($content){
		$int = unpack('N', substr($content, 0, 4));
		return $int[1];
	}

	/**
	 * This adds the content-length to the content that is about to be written over the EPP Protocol
	 *
	 * @param string $content Your XML
	 * @return string String to write
	 */
	private function addInteger($content){
		$int = pack('N',intval(strlen($content)+4));
		return $int.$content;
	}

	/**
	 * Write stuff over the connection
	 * @param string $content
	 * @return boolean
	 */
	public function write($content){   
		$this->writeLog("Writing: ".strlen($content)." + 4 bytes");
		//$content = $this->addInteger($content);
		if (!is_resource($this->connection)){   
			return false;
		}
		$this->writeLog($content);
		putenv('SURPRESS_ERROR_HANDLER=1');
		#echo $content;
		#ob_flush();
		if (fwrite($this->connection, $content)){   
			//fpassthru($this->connection);
			putenv('SURPRESS_ERROR_HANDLER=0');
			return true;
		}
		putenv('SURPRESS_ERROR_HANDLER=0');
		return false;
	}

	/**
	 * executeCQL
	 * Run a cql through the proxy
	 * @param string cql for cassandra db
	 * @return array count=<#results found>
	 **/
	public function executeCQL($cql){
		$ret = array();
		$ret['success']=false;

		if($this->isConnected()){
			$this->writeLog("Execute: $cql");
			$ar['action']="execute";
			$ar['cql']=$cql;
			if($this->write(json_encode($ar))){
				$buffer=$this->read();
				if(!empty($buffer)){
					$this->queryData=json_decode($buffer,true);
					$ret=$this->queryData;
					$this->error=null;
					$ret['success']=true;
				}else{
					$this->error=1;
					$ret['message']='No response from proxy';
					$ret['success']=false;
				}
			}else{
				$this->error=1;
				$ret['message']='Timeout proxy';
				$ret['success']=false;
			}
		}else{
			$this->error=1;
			$ret['message']='Not connected';
		}
		return $ret;
	}

	/**
	 * next
	 * Fetches the next record through the proxy
	 * @param string cql for cassandra db
	 * @return bool 0=done; 1=data is set;
	 **/
	public function next(){
		if($this->isConnected()){
			$this->writeLog("Next");
			$ar['action']="next";
			$this->write(json_encode($ar));
			$buffer=$this->read();
			if(!empty($buffer)){
				$this->recordData=json_decode($buffer,true);
				//print "<pre>"; print_r($this->recordData);
				if(isset($this->recordData['result']) && !empty($this->recordData['result'])){
					$this->error=1;
					return 1;
				}else{
					$this->error=null;
					return 0;
				}
			}
		}
		return 0;
	}

	/**
	 * get
	 * Gets the record data
	 * @param string fieldname
	 * @return mixed data belonging to the fieldname
	 **/
	public function get($field){
		if(isset($this->recordData['result'][$field])){
			$value=$this->recordData['result'][$field];
		}else $value='';
		return $value;
	}

	/**
	 * getRecord
	 * Gets the complete currenct record data
	 * @return mixed data belonging to the fieldname
	 **/
	public function getRecord(){
		return  $this->recordData;
	}

	/**
	 * getRowCount
	 * Gets the rowcount of the requested query or 0 if no rows are found
	 * @return mixed data belonging to the fieldname
	 **/
	public function getRowCount(){
		if(isset($this->queryData['row_count']))
			return  $this->queryData['row_count'];
		else
			return 0;
	}


	/**
	 * getError
	 * returns a false if there is an error encounterd
	 * @return bool null=ok; false=error;
	 **/
	public function getError(){
		return $this->error;
	}

	/**
	 * escape
	 * escapes the string for cassandra usage
	 * @param string value to be escaped
	 * @return string escaped value
	 **/
	public function escape($value){
		return str_replace("'","''",$value);
	}

	public function enableLogging(){
		date_default_timezone_set("Europe/Amsterdam");
		$this->logging = true;
	}

	private function sendLog($email, $subject){
		mail($email, $subject, implode("\n", $this->logentries));
	}

	public function showLog(){
		echo "==== LOG ====";
		if (property_exists($this,'logentries')){
			print_r($this->logentries);
		}
	}

	private function writeLog($text){
		if ($this->logging){
			$this->logentries[] = "-----".date("Y-m-d H:i:s")."-----".$text."-----end-----\n";
		}
	}

}
?>
