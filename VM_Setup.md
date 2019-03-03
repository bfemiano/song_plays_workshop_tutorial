
The VM will give you access to Java, Scala, Gradle, Python 2.7, Luigi, and Spark 2.2.1 all in one environment. 

It will be useful for compiling and running code you develop locally without having to clutter your machine
with these dependencies. 

Make sure you you have Vagrant 2.2.2+ and VirtualBox 6.0+ installed. 

## Install the VM from the .box file. 

1. Paste the following link into your browser: https://drive.google.com/uc?export=download&confirm=B1GP&id=1cvjqGJxUzDP_puZHd5E3gSeEYnnv44kE
2. Click "Download anyway" on the file prompt. The box file doesn't contain any malware. 
3. `vagrant box add --name bfemiano/song_plays_tutorial /some/path/to/song_plays_tutorial.box`
4. Choose a directory on your local machine to be your workspace location. Change to this directory and run `vagrant init bfemiano/song_plays_tutorial`
5. Add the below lines to VagrantFile on the line right below the config.vm.box config. 
```
config.ssh.username="student"
config.ssh.password="password"
```

6. `vagrant up`
7. `vagrant ssh`


## File mount point.
Files placed in the same directory as the Vagrantfile will appear on in the VM under the mount `/vagrant`

## To ssh into vagrant box without vagrant ssh
`ssh student@127.0.0.1 -p 2222`

## Alternative methods to move data into the vagrant box:
Method 1 scp. `scp -P 2222 -r some_dir student@127.0.0.1:./some_dir`

Method 2 rsync. `rsync -e "ssh -p 2222" -av some_file.ext student@127.0.0.1:.`

Using the VM `/vagrant` mount is by far the easiest method. 