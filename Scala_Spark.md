Assuming working dir under /vagrant

cp -r ~/gradle /vagrant/{scala_location}
cp ~/gradlew  /vagrant/{scala_location}

Mention mv the shadow jar built by gradle under scala_location/build/libs to the working dir with the name 'song_plays.jar'