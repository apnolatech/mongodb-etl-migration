"""
Encryption/decryption module for message migration
Supports:
- Salsa20 decryption (MongoDB old system)
- AES-256-CBC encryption (new system)
"""

import base64
import hashlib
import json
import logging
from typing import Optional, Tuple
from Crypto.Cipher import AES, Salsa20
from Crypto.Util.Padding import pad, unpad
from Crypto.Random import get_random_bytes

logger = logging.getLogger(__name__)


class OldEncryption:
    """Old system decryption (Salsa20)"""
    
    def __init__(self, key: str, iv: str):
        """
        Initializes old decryptor
        
        Args:
            key: Encryption key (oldchat.key)
            iv: Initialization vector (oldchat.iv)
        """
        self.key = self._prepare_key(key)
        self.iv = self._prepare_iv(iv)
    
    def _prepare_key(self, key: str) -> bytes:
        """Prepares key for Salsa20 (32 bytes)"""
        key_bytes = key.encode('utf-8')
        if len(key_bytes) > 32:
            return key_bytes[:32]
        elif len(key_bytes) < 32:
            # Padding with zeros
            padded = bytearray(32)
            padded[:len(key_bytes)] = key_bytes
            return bytes(padded)
        return key_bytes
    
    def _prepare_iv(self, iv: str) -> bytes:
        """Prepares IV for Salsa20 (8 bytes)"""
        iv_bytes = iv.encode('utf-8')
        if len(iv_bytes) > 8:
            return iv_bytes[:8]
        elif len(iv_bytes) < 8:
            # Padding with zeros
            padded = bytearray(8)
            padded[:len(iv_bytes)] = iv_bytes
            return bytes(padded)
        return iv_bytes
    
    def decrypt(self, encrypted_base64: str) -> Optional[str]:
        """
        Decrypts a message from old system
        
        Args:
            encrypted_base64: Encrypted message in base64
            
        Returns:
            Decrypted message or None if fails
        """
        if not encrypted_base64:
            return ""
        
        try:
            # Decode base64
            encrypted = base64.b64decode(encrypted_base64)
            
            # Create Salsa20 cipher
            cipher = Salsa20.new(key=self.key, nonce=self.iv)
            
            # Decrypt
            decrypted = cipher.decrypt(encrypted)
            
            # Try to decode UTF-8
            try:
                return decrypted.decode('utf-8')
            except UnicodeDecodeError:
                # Message might not be encrypted, try returning original message
                logger.debug(f"Decrypted message is not valid UTF-8, might not be encrypted")
                return None
                
        except Exception as e:
            logger.debug(f"Error decrypting with Salsa20: {e}")
            return None


class NewEncryption:
    """New system encryption (AES-256-CBC)"""
    
    def __init__(self, master_key: str, master_iv: str):
        """
        Initializes new encryptor
        
        Args:
            master_key: Master key in hexadecimal (chat.key)
            master_iv: Master IV in hexadecimal (chat.iv)
        """
        self.master_key = bytes.fromhex(master_key)
        self.master_iv = bytes.fromhex(master_iv)
    
    def _pkcs7_pad(self, data: bytes) -> bytes:
        """Applies PKCS7 padding"""
        padding_length = 16 - (len(data) % 16)
        padding = bytes([padding_length] * padding_length)
        return data + padding
    
    def _pkcs7_unpad(self, data: bytes) -> bytes:
        """Removes PKCS7 padding"""
        padding_length = data[-1]
        return data[:-padding_length]
    
    def generate_encryption_data(self, password: str = "some password") -> Tuple[str, str, str]:
        """
        Generates new key, IV and encryptionData for a message using password-based derivation
        
        Args:
            password: Password to derive key and IV from (default: "some password")
        
        Returns:
            Tuple (key_hex, iv_hex, encryption_data_base64)
        """
        # Derive key and IV from password using SHA256
        # This ensures deterministic encryption keys based on the password
        import hashlib
        
        # Derive 32-byte key from password
        key = hashlib.sha256(password.encode('utf-8')).digest()
        key_hex = key.hex()
        
        # Derive 16-byte IV from password (use different salt)
        iv_source = hashlib.sha256((password + "_iv").encode('utf-8')).digest()
        iv = iv_source[:16]  # Take first 16 bytes
        iv_hex = iv.hex()
        
        # Create JSON with key and IV
        data_json = json.dumps({
            "key": key_hex,
            "iv": iv_hex
        })
        
        # Encrypt JSON with master key
        encrypted_data = self._encrypt_with_master(data_json)
        
        # Convert to base64
        encryption_data = base64.b64encode(bytes.fromhex(encrypted_data)).decode('utf-8')
        
        return key_hex, iv_hex, encryption_data
    
    def _encrypt_with_master(self, data: str) -> str:
        """Encrypts data with master key (for encryptionData)"""
        # Convert string to bytes and apply padding
        data_bytes = data.encode('utf-8')
        padded_data = self._pkcs7_pad(data_bytes)
        
        # Create AES cipher in CBC mode
        cipher = AES.new(self.master_key, AES.MODE_CBC, self.master_iv)
        
        # Encrypt
        encrypted = cipher.encrypt(padded_data)
        
        # Return in hexadecimal
        return encrypted.hex()
    
    def _decrypt_with_master(self, encrypted_hex: str) -> dict:
        """Decrypts encryptionData with master key"""
        # Convert hex to bytes
        encrypted = bytes.fromhex(encrypted_hex)
        
        # Create AES cipher in CBC mode
        cipher = AES.new(self.master_key, AES.MODE_CBC, self.master_iv)
        
        # Decrypt
        decrypted_padded = cipher.decrypt(encrypted)
        
        # Remove padding
        decrypted = self._pkcs7_unpad(decrypted_padded)
        
        # Parse JSON
        data_json = json.loads(decrypted.decode('utf-8'))
        
        return data_json
    
    def encrypt(self, message: str, encryption_data: str) -> str:
        """
        Encrypts a message with new system
        
        Args:
            message: Message to encrypt
            encryption_data: EncryptionData in base64
            
        Returns:
            Encrypted message in base64
        """
        if not message:
            return ""
        
        # Decode encryptionData from base64 to bytes
        encryption_bytes = base64.b64decode(encryption_data)
        encryption_hex = encryption_bytes.hex()
        
        # Decrypt encryptionData to get key and IV
        data_json = self._decrypt_with_master(encryption_hex)
        key = bytes.fromhex(data_json['key'])
        iv = bytes.fromhex(data_json['iv'])
        
        # Prepare message
        message_bytes = message.encode('utf-8')
        padded_message = self._pkcs7_pad(message_bytes)
        
        # Create AES cipher in CBC mode
        cipher = AES.new(key, AES.MODE_CBC, iv)
        
        # Encrypt
        encrypted = cipher.encrypt(padded_message)
        
        # Convert directly to base64
        encrypted_base64 = base64.b64encode(encrypted).decode('utf-8')
        
        return encrypted_base64
    
    def decrypt(self, encrypted_base64: str, encryption_data: str) -> Optional[str]:
        """
        Decrypts a message from new system (for testing)
        
        Args:
            encrypted_base64: Encrypted message in base64
            encryption_data: EncryptionData in base64
            
        Returns:
            Decrypted message or None if fails
        """
        if not encrypted_base64:
            return ""
        
        try:
            # Decode encryptionData from base64 to bytes
            encryption_bytes = base64.b64decode(encryption_data)
            encryption_hex = encryption_bytes.hex()
            
            # Decrypt encryptionData to get key and IV
            data_json = self._decrypt_with_master(encryption_hex)
            key = bytes.fromhex(data_json['key'])
            iv = bytes.fromhex(data_json['iv'])
            
            # Decode message directly from base64 to bytes
            encrypted = base64.b64decode(encrypted_base64)
            
            # Validate it's a multiple of 16 bytes
            if len(encrypted) % 16 != 0:
                raise ValueError("Encrypted data length is not a multiple of 16")
            
            # Create AES cipher in CBC mode
            cipher = AES.new(key, AES.MODE_CBC, iv)
            
            # Decrypt
            decrypted_padded = cipher.decrypt(encrypted)
            
            # Remove padding
            decrypted = self._pkcs7_unpad(decrypted_padded)
            
            return decrypted.decode('utf-8')
        except Exception as e:
            logger.debug(f"Error decrypting with AES: {e}")
            return None


class EncryptionMigrator:
    """Encryption migration manager"""
    
    def __init__(self, old_key: str, old_iv: str, new_key: str, new_iv: str):
        """
        Initializes encryption migrator
        
        Args:
            old_key: Old key (oldchat.key)
            old_iv: Old IV (oldchat.iv)
            new_key: New master key (chat.key in hex)
            new_iv: New master IV (chat.iv in hex)
        """
        self.old_encryption = OldEncryption(old_key, old_iv)
        self.new_encryption = NewEncryption(new_key, new_iv)
    
    def migrate_message(self, encrypted_message: str, log_decrypted: bool = True) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Migrates a message from old system to new
        
        Args:
            encrypted_message: Message encrypted with old system
            log_decrypted: If True, prints decrypted message in logs
            
        Returns:
            Tuple (decrypted_message, new_encrypted_message, encryption_data)
            or (None, None, None) if fails
        """
        if not encrypted_message:
            return "", "", ""
        
        # 1. Decrypt with old system
        decrypted = self.old_encryption.decrypt(encrypted_message)
        
        if decrypted is None:
            logger.debug(f"Could not decrypt message (might not be encrypted): {encrypted_message[:50]}...")
            # If cannot decrypt, message might not be encrypted
            # In that case, use original message as plain text
            return encrypted_message, None, None
        
        # 2. Log decrypted message (for debugging)
        if log_decrypted:
            logger.debug(f"Decrypted message: {decrypted[:100]}{'...' if len(decrypted) > 100 else ''}")
        
        # 3. Generate new encryptionData using password "some password"
        key_hex, iv_hex, encryption_data = self.new_encryption.generate_encryption_data(password="some password")
        
        # 4. Encrypt with new system
        new_encrypted = self.new_encryption.encrypt(decrypted, encryption_data)
        
        return decrypted, new_encrypted, encryption_data
    
    def test_migration(self, original_encrypted: str) -> bool:
        """
        Tests that migration works correctly
        
        Args:
            original_encrypted: Original encrypted message
            
        Returns:
            True if migration is successful
        """
        # Migrate
        decrypted, new_encrypted, encryption_data = self.migrate_message(
            original_encrypted, 
            log_decrypted=False
        )
        
        if decrypted is None:
            return False
        
        # Verify we can decrypt with new system
        re_decrypted = self.new_encryption.decrypt(new_encrypted, encryption_data)
        
        if re_decrypted != decrypted:
            logger.error(f"Test failed: {decrypted} != {re_decrypted}")
            return False
        
        logger.info(f"Test successful: Message migrated correctly")
        return True

