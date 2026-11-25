import { createHash } from 'node:crypto';
import { chmodSync, lstatSync, mkdirSync } from 'node:fs';
import { homedir } from 'node:os';
import path from 'node:path';
import { BridgeError } from '../../shared/errors.js';

                                       
export function persistentSocketLocation(home: string, socketSuffix: string, userHome = homedir()): string {
  const root = path.resolve(home);
  const fits = (directory: string) => Buffer.byteLength(path.join(directory, socketSuffix)) <= 103;
  let directory = path.join(root, 'run');
  if (!fits(directory)) {
    const key = createHash('sha256').update(root).digest('hex').slice(0, 12);
    directory = path.resolve(userHome, `.nxb-${key}`);
    if (!fits(directory)) throw new BridgeError('socket_path_too_long', '持久 Unix socket 路径过长，请将 NXBRIDGE_HOME 设置为较短的绝对路径。');
  }
  return directory;
}

export function persistentSocketDirectory(home: string, socketSuffix: string, userHome = homedir()): string {
  const directory = persistentSocketLocation(home, socketSuffix, userHome);
  mkdirSync(directory, { recursive: true, mode: 0o700 });
  const stat = lstatSync(directory);
  if (!stat.isDirectory() || stat.uid !== process.getuid?.()) {
    throw new BridgeError('socket_directory_conflict', `持久 Unix socket 需要当前用户拥有的实际目录：${directory}`);
  }
                                
  if ((stat.mode & 0o777) !== 0o700) chmodSync(directory, 0o700);
  return directory;
}
