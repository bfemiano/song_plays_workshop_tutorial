
The VM will give you access to Java, Scala, Gradle, Python 2.7, Luigi, and Spark 2.2.1. 

It will be useful for compiling and running code you develop locally without having to clutter your machine
with these dependencies. 

Note: Whether installing directly from the song_plays_tutorial.box file for using the JSON conf, make sure you you have Vagrant 2.2.2+ and VirtualBox 6.0+ installed. 

## From github JSON conf file. 

1. `wget https://github.com/bfemiano/song_plays_workshop_tutorial/blob/master/song_plays_tutorial.json`  
2. `vagrant box add song_plays_tutorial.json`
3. Choose a directory on your local machine to be your workspace location. Change to this directory and run `vagrant init bfemiano/song_plays_tutorial`
4. Add the below lines to VagrantFile on the line right below the config.vm.box config. 
```
config.ssh.username="student"
config.ssh.password="password"
```

5. `vagrant up`
6. `vagrant ssh`


## Directly from the .box file. 
### This can be useful if running over limited bandwidth. See instructor for a copy of the .box file. 

1. `vagrant box add --name bfemiano/song_plays_tutorial /some/path/to/song_plays_tutorial.box`
2. Choose a directory on your local machine to be your workspace location. Change to this directory and run `vagrant init bfemiano/song_plays_tutorial`
3. Add the below lines to VagrantFile on the line right below the config.vm.box config. 
```
config.ssh.username="student"
config.ssh.password="password"
```

4. `vagrant up`
5. `vagrant ssh`


## File mount point.
Files placed in the same directory as the Vagrantfile will appear on the Guest OS under the location `/vagrant`

## To ssh into vagrant box without vagrant ssh
`ssh student@127.0.0.1 -p 2222`

## To move data into the vagrant box:
`scp -P 2222 -r some_dir student@127.0.0.1:./some_dir`
or 
`rsync -e "ssh -p 2222" -av some_file.ext student@127.0.0.1:.`

