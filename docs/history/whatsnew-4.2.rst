.. _whatsnew-4.2:

===========================================
 What's new in Celery 4.2 (windowlicker)
===========================================
:Author: Omer Katz (``omer.drow at gmail.com``)

.. sidebar:: Change history

    What's new documents describe the changes in major versions,
    we also have a :ref:`changelog` that lists the changes in bugfix
    releases (0.0.x), while older series are archived under the :ref:`history`
    section.

Celery is a simple, flexible, and reliable distributed system to
process vast amounts of messages, while providing operations with
the tools required to maintain such a system.

It's a task queue with focus on real-time processing, while also
supporting task scheduling.

Celery has a large and diverse community of users and contributors,
you should come join us :ref:`on IRC <irc-channel>`
or :ref:`our mailing-list <mailing-list>`.

To read more about Celery you should go read the :ref:`introduction <intro>`.

While this version is backward compatible with previous versions
it's important that you read the following section.

This version is officially supported on CPython 2.7, 3.4, 3.5 & 3.6
and is also supported on PyPy.

.. _`website`: http://celeryproject.org/

.. topic:: Table of Contents

    Make sure you read the important notes before upgrading to this version.

.. contents::
    :local:
    :depth: 2

Preface
=======

The 4.2.0 release continues to improve our efforts to provide you with
the best task execution platform for Python.

This release is mainly a bug fix release, ironing out some issues and regressions
found in Celery 4.0.0.

Traditionally, releases were named after `Autechre <https://en.wikipedia.org/wiki/Autechre>`_'s track names.
This release continues this tradition in a slightly different way.
Each major version of Celery will use a different artist's track names as codenames.

From now on, the 4.x series will be codenamed after `Aphex Twin <https://en.wikipedia.org/wiki/Aphex_Twin>`_'s track names.
This release is codenamed after his very famous track, `Windowlicker <https://youtu.be/UBS4Gi1y_nc?t=4m>`_.

Thank you for your support!

*— Omer Katz*

Wall of Contributors
--------------------

Aaron Harnly <aharnly@wgen.net>
Aaron Harnly <github.com@bulk.harnly.net>
Aaron McMillin <github@aaron.mcmillinclan.org>
Aaron Ross <aaronelliotross@gmail.com>
Aaron Ross <aaron@wawd.com>
Aaron Schumacher <ajschumacher@gmail.com>
abecciu <augusto@becciu.org>
abhinav nilaratna <anilaratna2@bloomberg.net>
Acey9 <huiwang.e@gmail.com>
Acey <huiwang.e@gmail.com>
aclowes <aclowes@gmail.com>
Adam Chainz <adam@adamj.eu>
Adam DePue <adepue@hearsaycorp.com>
Adam Endicott <adam@zoey.local>
Adam Renberg <tgwizard@gmail.com>
Adam Venturella <aventurella@gmail.com>
Adaptification <Adaptification@users.noreply.github.com>
Adrian <adrian@planetcoding.net>
adriano petrich <petrich@gmail.com>
Adrian Rego <arego320@gmail.com>
Adrien Guinet <aguinet@quarkslab.com>
Agris Ameriks <ameriks@gmail.com>
Ahmet Demir <ahmet2mir+github@gmail.com>
air-upc <xin.shli@ele.me>
Aitor Gómez-Goiri <aitor@gomezgoiri.net>
Akira Matsuzaki <akira.matsuzaki.1977@gmail.com>
Akshar Raaj <akshar@agiliq.com>
Alain Masiero <amasiero@ocs.online.net>
Alan Hamlett <alan.hamlett@prezi.com>
Alan Hamlett <alanhamlett@users.noreply.github.com>
Alan Justino <alan.justino@yahoo.com.br>
Alan Justino da Silva <alan.justino@yahoo.com.br>
Albert Wang <albert@zerocater.com>
Alcides Viamontes Esquivel <a.viamontes.esquivel@gmail.com>
Alec Clowes <aclowes@gmail.com>
Alejandro Pernin <ale.pernin@gmail.com>
Alejandro Varas <alej0varas@gmail.com>
Aleksandr Kuznetsov <aku.ru.kz@gmail.com>
Ales Zoulek <ales.zoulek@gmail.com>
Alexander <a.a.lebedev@gmail.com>
Alexander A. Sosnovskiy <alecs.box@gmail.com>
Alexander Koshelev <daevaorn@gmail.com>
Alexander Koval <kovalidis@gmail.com>
Alexander Oblovatniy <oblalex@users.noreply.github.com>
Alexander Oblovatniy <oblovatniy@gmail.com>
Alexander Ovechkin <frostoov@gmail.com>
Alexander Smirnov <asmirnov@five9.com>
Alexandru Chirila <alex@alexkiro.com>
Alexey Kotlyarov <alexey@infoxchange.net.au>
Alexey Zatelepin <ztlpn@yandex-team.ru>
Alex Garel <alex@garel.org>
Alex Hill <alex@hill.net.au>
Alex Kiriukha <akiriukha@cogniance.com>
Alex Koshelev <daevaorn@gmail.com>
Alex Rattray <rattray.alex@gmail.com>
Alex Williams <alex.williams@skyscanner.net>
Alex Zaitsev <azaitsev@gmail.com>
Ali Bozorgkhan <alibozorgkhan@gmail.com>
Allan Caffee <allan.caffee@gmail.com>
Allard Hoeve <allard@byte.nl>
allenling <lingyiwang@haomaiyi.com>
Alli <alzeih@users.noreply.github.com>
Alman One <alman@laptop.home>
Alman One <alman-one@laptop.home>
alman-one <masiero.alain@gmail.com>
Amir Rustamzadeh <amirrustam@users.noreply.github.com>
anand21nanda@gmail.com <anand21nanda@gmail.com>
Anarchist666 <Anarchist666@yandex.ru>
Anders Pearson <anders@columbia.edu>
Andrea Rabbaglietti <silverfix@gmail.com>
Andreas Pelme <andreas@pelme.se>
Andreas Savvides <andreas@editd.com>
Andrei Fokau <andrei.fokau@neutron.kth.se>
Andrew de Quincey <adq@lidskialf.net>
Andrew Kittredge <andrewlkittredge@gmail.com>
Andrew McFague <amcfague@wgen.net>
Andrew Stewart <astewart@twistbioscience.com>
Andrew Watts <andrewwatts@gmail.com>
Andrew Wong <argsno@gmail.com>
Andrey Voronov <eyvoro@users.noreply.github.com>
Andriy Yurchuk <ayurchuk@minuteware.net>
Aneil Mallavarapu <aneil.mallavar@gmail.com>
anentropic <ego@anentropic.com>
anh <anhlh2@gmail.com>
Ankur Dedania <AbsoluteMSTR@gmail.com>
Anthony Lukach <anthonylukach@gmail.com>
antlegrand <2t.antoine@gmail.com>
Antoine Legrand <antoine.legrand@smartjog.com>
Anton <anton.gladkov@gmail.com>
Anton Gladkov <atn18@yandex-team.ru>
Antonin Delpeuch <antonin@delpeuch.eu>
Arcadiy Ivanov <arcadiy@ivanov.biz>
areski <areski@gmail.com>
Armenak Baburyan <kanemra@gmail.com>
Armin Ronacher <armin.ronacher@active-4.com>
armo <kanemra@gmail.com>
Arnaud Rocher <cailloumajor@users.noreply.github.com>
arpanshah29 <ashah29@stanford.edu>
Arsenio Santos <arsenio@gmail.com>
Arthur Vigil <ahvigil@mail.sfsu.edu>
Arthur Vuillard <arthur@hashbang.fr>
Ashish Dubey <ashish.dubey91@gmail.com>
Asif Saifuddin Auvi <auvipy@gmail.com>
Asif Saifuddin Auvi <auvipy@users.noreply.github.com>
ask <ask@0x61736b.net>
Ask Solem <ask@celeryproject.org>
Ask Solem <askh@opera.com>
Ask Solem Hoel <ask@celeryproject.org>
aydin <adigeaydin@gmail.com>
baeuml <baeuml@kit.edu>
Balachandran C <balachandran.c@gramvaani.org>
Balthazar Rouberol <balthazar.rouberol@mapado.com>
Balthazar Rouberol <balthazar.rouberol@ubertas.co.uk>
bartloop <38962178+bartloop@users.noreply.github.com>
Bartosz Ptaszynski <>
Batiste Bieler <batiste.bieler@pix4d.com>
bee-keeper <ricbottomley@gmail.com>
Bence Tamas <mr.bence.tamas@gmail.com>
Ben Firshman <ben@firshman.co.uk>
Ben Welsh <ben.welsh@gmail.com>
Berker Peksag <berker.peksag@gmail.com>
Bert Vanderbauwhede <batlock666@gmail.com>
Bert Vanderbauwhede <bert.vanderbauwhede@ugent.be>
BLAGA Razvan-Paul <razvan.paul.blaga@gmail.com>
bobbybeever <bobby.beever@yahoo.com>
bobby <bobby.beever@yahoo.com>
Bobby Powers <bobbypowers@gmail.com>
Bohdan Rybak <bohdan.rybak@gmail.com>
Brad Jasper <bjasper@gmail.com>
Branko Čibej <brane@apache.org>
BR <b.rabiega@gmail.com>
Brendan MacDonell <macdonellba@gmail.com>
Brendon Crawford <brendon@aphexcreations.net>
Brent Watson <brent@brentwatson.com>
Brian Bouterse <bmbouter@gmail.com>
Brian Dixon <bjdixon@gmail.com>
Brian Luan <jznight@gmail.com>
Brian May <brian@linuxpenguins.xyz>
Brian Peiris <brianpeiris@gmail.com>
Brian Rosner <brosner@gmail.com>
Brodie Rao <brodie@sf.io>
Bruno Alla <browniebroke@users.noreply.github.com>
Bryan Berg <bdb@north-eastham.org>
Bryan Berg <bryan@mixedmedialabs.com>
Bryan Bishop <kanzure@gmail.com>
Bryan Helmig <bryan@bryanhelmig.com>
Bryce Groff <bgroff@hawaii.edu>
Caleb Mingle <mingle@uber.com>
Carlos Garcia-Dubus <carlos.garciadm@gmail.com>
Catalin Iacob <iacobcatalin@gmail.com>
Charles McLaughlin <mclaughlinct@gmail.com>
Chase Seibert <chase.seibert+github@gmail.com>
ChillarAnand <anand21nanda@gmail.com>
Chris Adams <chris@improbable.org>
Chris Angove <cangove@wgen.net>
Chris Chamberlin <chamberlincd@gmail.com>
chrisclark <chris@untrod.com>
Chris Harris <chris.harris@kitware.com>
Chris Kuehl <chris@techxonline.net>
Chris Martin <ch.martin@gmail.com>
Chris Mitchell <chris.mit7@gmail.com>
Chris Rose <offby1@offby1.net>
Chris St. Pierre <chris.a.st.pierre@gmail.com>
Chris Streeter <chris@chrisstreeter.com>
Christian <github@penpal4u.net>
Christoph Burgmer <christoph@nwebs.de>
Christopher Hoskin <mans0954@users.noreply.github.com>
Christopher Lee <chris@cozi.com>
Christopher Peplin <github@rhubarbtech.com>
Christopher Peplin <peplin@bueda.com>
Christoph Krybus <ckrybus@googlemail.com>
clayg <clay.gerrard@gmail.com>
Clay Gerrard <clayg@clayg-desktop.(none)>
Clemens Wolff <clemens@justamouse.com>
cmclaughlin <mclaughlinct@gmail.com>
Codeb Fan <codeb2cc@gmail.com>
Colin McIntosh <colin@colinmcintosh.com>
Conrad Kramer <ckrames1234@gmail.com>
Corey Farwell <coreyf@rwell.org>
Craig Younkins <cyounkins@Craigs-MacBook-Pro.local>
csfeathers <csfeathers@users.noreply.github.com>
Cullen Rhodes <rhodes.cullen@yahoo.co.uk>
daftshady <daftonshady@gmail.com>
Dan <dmtaub@gmail.com>
Dan Hackner <dan.hackner@gmail.com>
Daniel Devine <devine@ddevnet.net>
Daniele Procida <daniele@vurt.org>
Daniel Hahler <github@thequod.de>
Daniel Hepper <daniel.hepper@gmail.com>
Daniel Huang <dxhuang@gmail.com>
Daniel Lundin <daniel.lundin@trioptima.com>
Daniel Lundin <dln@eintr.org>
Daniel Watkins <daniel@daniel-watkins.co.uk>
Danilo Bargen <mail@dbrgn.ch>
Dan McGee <dan@archlinux.org>
Dan McGee <dpmcgee@gmail.com>
Dan Wilson <danjwilson@gmail.com>
Daodao <daodaod@gmail.com>
Dave Smith <dave@thesmithfam.org>
Dave Smith <dsmith@hirevue.com>
David Arthur <darthur@digitalsmiths.com>
David Arthur <mumrah@gmail.com>
David Baumgold <david@davidbaumgold.com>
David Cramer <dcramer@gmail.com>
David Davis <daviddavis@users.noreply.github.com>
David Harrigan <dharrigan118@gmail.com>
David Harrigan <dharrigan@dyn.com>
David Markey <dmarkey@localhost.localdomain>
David Miller <david@deadpansincerity.com>
David Miller <il.livid.dream@gmail.com>
David Pravec <David.Pravec@danix.org>
David Pravec <david.pravec@nethost.cz>
David Strauss <david@davidstrauss.net>
David White <dpwhite2@ncsu.edu>
DDevine <devine@ddevnet.net>
Denis Podlesniy <Haos616@Gmail.com>
Denis Shirokov <dan@rexuni.com>
Dennis Brakhane <dennis.brakhane@inoio.de>
Derek Harland <donkopotamus@users.noreply.github.com>
derek_kim <bluewhale8202@gmail.com>
dessant <dessant@users.noreply.github.com>
Dieter Adriaenssens <ruleant@users.sourceforge.net>
Dima Kurguzov <koorgoo@gmail.com>
dimka665 <dimka665@gmail.com>
dimlev <dimlev@gmail.com>
dmarkey <david@dmarkey.com>
Dmitry Malinovsky <damalinov@gmail.com>
Dmitry Malinovsky <dmalinovsky@thumbtack.net>
dmollerm <d.moller.m@gmail.com>
Dmytro Petruk <bavaria95@gmail.com>
dolugen <dolugen@gmail.com>
dongweiming <ciici1234@hotmail.com>
dongweiming <ciici123@gmail.com>
Dongweiming <ciici123@gmail.com>
dtheodor <dimitris.theodorou@gmail.com>
Dudás Ádám <sir.dudas.adam@gmail.com>
Dustin J. Mitchell <dustin@mozilla.com>
D. Yu <darylyu@users.noreply.github.com>
Ed Morley <edmorley@users.noreply.github.com>
Eduardo Ramírez <ejramire@uc.cl>
Edward Betts <edward@4angle.com>
Emil Stanchev <stanchev.emil@gmail.com>
Eran Rundstein <eran@sandsquid.(none)>
ergo <ergo@debian.Belkin>
Eric Poelke <epoelke@gmail.com>
Eric Zarowny <ezarowny@gmail.com>
ernop <ernestfrench@gmail.com>
Evgeniy <quick.es@gmail.com>
evildmp <daniele@apple-juice.co.uk>
fatihsucu <fatihsucu0@gmail.com>
Fatih Sucu <fatihsucu@users.noreply.github.com>
Feanil Patel <feanil@edx.org>
Felipe <fcoelho@users.noreply.github.com>
Felipe Godói Rosário <felipe.rosario@geru.com.br>
Felix Berger <bflat1@gmx.net>
Fengyuan Chen <cfy1990@gmail.com>
Fernando Rocha <fernandogrd@gmail.com>
ffeast <ffeast@gmail.com>
Flavio Percoco Premoli <flaper87@gmail.com>
Florian Apolloner <apollo13@apolloner.eu>
Florian Apolloner <florian@apollo13.(none)>
Florian Demmer <fdemmer@gmail.com>
flyingfoxlee <lingyunzhi312@gmail.com>
Francois Visconte <f.visconte@gmail.com>
François Voron <fvoron@gmail.com>
Frédéric Junod <frederic.junod@camptocamp.com>
fredj <frederic.junod@camptocamp.com>
frol <frolvlad@gmail.com>
Gabriel <gabrielpjordao@gmail.com>
Gao Jiangmiao <gao.jiangmiao@h3c.com>
GDR! <gdr@gdr.name>
GDvalle <GDvalle@users.noreply.github.com>
Geoffrey Bauduin <bauduin.geo@gmail.com>
georgepsarakis <giwrgos.psarakis@gmail.com>
George Psarakis <giwrgos.psarakis@gmail.com>
George Sibble <gsibble@gmail.com>
George Tantiras <raratiru@users.noreply.github.com>
Georgy Cheshkov <medoslav@gmail.com>
Gerald Manipon <pymonger@gmail.com>
German M. Bravo <german.mb@deipi.com>
Gert Van Gool <gertvangool@gmail.com>
Gilles Dartiguelongue <gilles.dartiguelongue@esiee.org>
Gino Ledesma <gledesma@apple.com>
gmanipon <gmanipon@jpl.nasa.gov>
Grant Thomas <jgrantthomas@gmail.com>
Greg Haskins <greg@greghaskins.com>
gregoire <gregoire@audacy.fr>
Greg Taylor <gtaylor@duointeractive.com>
Greg Wilbur <gwilbur@bloomberg.net>
Guillaume Gauvrit <guillaume@gandi.net>
Guillaume Gendre <dzb.rtz@gmail.com>
Gun.io Whitespace Robot <contact@gun.io>
Gunnlaugur Thor Briem <gunnlaugur@gmail.com>
harm <harm.verhagen@gmail.com>
Harm Verhagen <harm.verhagen@gmail.com>
Harry Moreno <morenoh149@gmail.com>
hclihn <23141651+hclihn@users.noreply.github.com>
hekevintran <hekevintran@gmail.com>
honux <atoahp@hotmail.com>
Honza Kral <honza.kral@gmail.com>
Honza Král <Honza.Kral@gmail.com>
Hooksie <me@matthooks.com>
Hsiaoming Yang <me@lepture.com>
Huang Huang <mozillazg101@gmail.com>
Hynek Schlawack <hs@ox.cx>
Hynek Schlawack <schlawack@variomedia.de>
Ian Dees <ian.dees@gmail.com>
Ian McCracken <ian.mccracken@gmail.com>
Ian Wilson <ian.owings@gmail.com>
Idan Kamara <idankk86@gmail.com>
Ignas Mikalajūnas <ignas.mikalajunas@gmail.com>
Igor Kasianov <super.hang.glider@gmail.com>
illes <illes.solt@gmail.com>
Ilya <4beast@gmail.com>
Ilya Georgievsky <i.georgievsky@drweb.com>
Ionel Cristian Mărieș <contact@ionelmc.ro>
Ionel Maries Cristian <contact@ionelmc.ro>
Ionut Turturica <jonozzz@yahoo.com>
Iurii Kriachko <iurii.kriachko@gmail.com>
Ivan Metzlar <metzlar@gmail.com>
Ivan Virabyan <i.virabyan@gmail.com>
j0hnsmith <info@whywouldwe.com>
Jackie Leng <Jackie.Leng@nelen-schuurmans.nl>
J Alan Brogan <jalanb@users.noreply.github.com>
Jameel Al-Aziz <me@jalaziz.net>
James M. Allen <james.m.allen@gmail.com>
James Michael DuPont <JamesMikeDuPont@gmail.com>
James Pulec <jpulec@gmail.com>
James Remeika <james@remeika.us>
Jamie Alessio <jamie@stoic.net>
Jannis Leidel <jannis@leidel.info>
Jared Biel <jared.biel@bolderthinking.com>
Jason Baker <amnorvend@gmail.com>
Jason Baker <jason@ubuntu.ubuntu-domain>
Jason Veatch <jtveatch@gmail.com>
Jasper Bryant-Greene <jbg@rf.net.nz>
Javier Domingo Cansino <javierdo1@gmail.com>
Javier Martin Montull <javier.martin.montull@cern.ch>
Jay Farrimond <jay@instaedu.com>
Jay McGrath <jaymcgrath@users.noreply.github.com>
jbiel <jared.biel@bolderthinking.com>
jbochi <jbochi@gmail.com>
Jed Smith <jed@jedsmith.org>
Jeff Balogh <github@jeffbalogh.org>
Jeff Balogh <me@jeffbalogh.org>
Jeff Terrace <jterrace@gmail.com>
Jeff Widman <jeff@jeffwidman.com>
Jelle Verstraaten <jelle.verstraaten@xs4all.nl>
Jeremy Cline <jeremy@jcline.org>
Jeremy Zafran <jeremy.zafran@cloudlock.com>
jerry <jerry@stellaservice.com>
Jerzy Kozera <jerzy.kozera@gmail.com>
Jerzy Kozera <jerzy.kozera@sensisoft.com>
jespern <jesper@noehr.org>
Jesper Noehr <jespern@jesper-noehrs-macbook-pro.local>
Jesse <jvanderdoes@gmail.com>
jess <jessachandler@gmail.com>
Jess Johnson <jess@grokcode.com>
Jian Yu <askingyj@gmail.com>
JJ <jairojair@gmail.com>
João Ricardo <joaoricardo000@gmail.com>
Jocelyn Delalande <jdelalande@oasiswork.fr>
JocelynDelalande <JocelynDelalande@users.noreply.github.com>
Joe Jevnik <JoeJev@gmail.com>
Joe Sanford <joe@cs.tufts.edu>
Joe Sanford <josephsanford@gmail.com>
Joey Wilhelm <tarkatronic@gmail.com>
John Anderson <sontek@gmail.com>
John Arnold <johnar@microsoft.com>
John Barham <jbarham@gmail.com>
John Watson <john@dctrwatson.com>
John Watson <john@disqus.com>
John Watson <johnw@mahalo.com>
John Whitlock <John-Whitlock@ieee.org>
Jonas Haag <jonas@lophus.org>
Jonas Obrist <me@ojii.ch>
Jonatan Heyman <jonatan@heyman.info>
Jonathan Jordan <jonathan@metaltoad.com>
Jonathan Sundqvist <sundqvist.jonathan@gmail.com>
jonathan vanasco <jonathan@2xlp.com>
Jon Chen <bsd@voltaire.sh>
Jon Dufresne <jon.dufresne@gmail.com>
Josh <kaizoku@phear.cc>
Josh Kupershmidt <schmiddy@gmail.com>
Joshua "jag" Ginsberg <jag@flowtheory.net>
Josue Balandrano Coronel <xirdneh@gmail.com>
Jozef <knaperek@users.noreply.github.com>
jpellerin <jpellerin@jpdesk.(none)>
jpellerin <none@none>
JP <jpellerin@gmail.com>
JTill <jtillman@hearsaycorp.com>
Juan Gutierrez <juanny.gee@gmail.com>
Juan Ignacio Catalano <catalanojuan@gmail.com>
Juan Rossi <juan@getmango.com>
Juarez Bochi <jbochi@gmail.com>
Jude Nagurney <jude@pwan.org>
Julien Deniau <julien@sitioweb.fr>
julienp <julien@caffeine.lu>
Julien Poissonnier <julien@caffeine.lu>
Jun Sakai <jsakai@splunk.com>
Justin Patrin <jpatrin@skyhighnetworks.com>
Justin Patrin <papercrane@reversefold.com>
Kalle Bronsen <bronsen@nrrd.de>
kamalgill <kamalgill@mac.com>
Kamil Breguła <mik-laj@users.noreply.github.com>
Kanan Rahimov <mail@kenanbek.me>
Kareem Zidane <kzidane@cs50.harvard.edu>
Keith Perkins <keith@tasteoftheworld.us>
Ken Fromm <ken@frommworldwide.com>
Ken Reese <krrg@users.noreply.github.com>
keves <e@keves.org>
Kevin Gu <guqi@reyagroup.com>
Kevin Harvey <kharvey@axialhealthcare.com>
Kevin McCarthy <me@kevinmccarthy.org>
Kevin Richardson <kevin.f.richardson@gmail.com>
Kevin Richardson <kevin@kevinrichardson.co>
Kevin Tran <hekevintran@gmail.com>
Kieran Brownlees <kbrownlees@users.noreply.github.com>
Kirill Pavlov <pavlov99@yandex.ru>
Kirill Romanov <djaler1@gmail.com>
komu <komuw05@gmail.com>
Konstantinos Koukopoulos <koukopoulos@gmail.com>
Konstantin Podshumok <kpp.live@gmail.com>
Kornelijus Survila <kornholijo@gmail.com>
Kouhei Maeda <mkouhei@gmail.com>
Kracekumar Ramaraju <me@kracekumar.com>
Krzysztof Bujniewicz <k.bujniewicz@bankier.pl>
kuno <neokuno@gmail.com>
Kxrr <Hi@Kxrr.Us>
Kyle Kelley <rgbkrk@gmail.com>
Laurent Peuch <cortex@worlddomination.be>
lead2gold <caronc@users.noreply.github.com>
Leo Dirac <leo@banyanbranch.com>
Leo Singer <leo.singer@ligo.org>
Lewis M. Kabui <lewis.maina@andela.com>
llllllllll <joejev@gmail.com>
Locker537 <Locker537@gmail.com>
Loic Bistuer <loic.bistuer@sixmedia.com>
Loisaida Sam <sam.sandberg@gmail.com>
lookfwd <lookfwd@gmail.com>
Loren Abrams <labrams@hearsaycorp.com>
Loren Abrams <loren.abrams@gmail.com>
Lucas Wiman <lucaswiman@counsyl.com>
lucio <lucio@prometeo.spirit.net.ar>
Luis Clara Gomez <ekkolabs@gmail.com>
Lukas Linhart <lukas.linhart@centrumholdings.com>
Łukasz Kożuchowski <lukasz.kozuchowski@10clouds.com>
Łukasz Langa <lukasz@langa.pl>
Łukasz Oleś <lukaszoles@gmail.com>
Luke Burden <lukeburden@gmail.com>
Luke Hutscal <luke@creaturecreative.com>
Luke Plant <L.Plant.98@cantab.net>
Luke Pomfrey <luke.pomfrey@titanemail.com>
Luke Zapart <drx@drx.pl>
mabouels <abouelsaoud@gmail.com>
Maciej Obuchowski <obuchowski.maciej@gmail.com>
Mads Jensen <mje@inducks.org>
Manuel Kaufmann <humitos@gmail.com>
Manuel Vázquez Acosta <mvaled@users.noreply.github.com>
Marat Sharafutdinov <decaz89@gmail.com>
Marcelo Da Cruz Pinto <Marcelo_DaCruzPinto@McAfee.com>
Marc Gibbons <marc_gibbons@rogers.com>
Marc Hörsken <mback2k@users.noreply.github.com>
Marcin Kuźmiński <marcin@python-blog.com>
marcinkuzminski <marcin@python-works.com>
Marcio Ribeiro <binary@b1n.org>
Marco Buttu <marco.buttu@gmail.com>
Marco Schweighauser <marco@mailrelay.ch>
mariia-zelenova <32500603+mariia-zelenova@users.noreply.github.com>
Marin Atanasov Nikolov <dnaeon@gmail.com>
Marius Gedminas <marius@gedmin.as>
mark hellewell <mark.hellewell@gmail.com>
Mark Lavin <markdlavin@gmail.com>
Mark Lavin <mlavin@caktusgroup.com>
Mark Parncutt <me@markparncutt.com>
Mark Story <mark@freshbooks.com>
Mark Stover <stovenator@gmail.com>
Mark Thurman <mthurman@gmail.com>
Markus Kaiserswerth <github@sensun.org>
Markus Ullmann <mail@markus-ullmann.de>
martialp <martialp@users.noreply.github.com>
Martin Davidsson <martin@dropcam.com>
Martin Galpin <m@66laps.com>
Martin Melin <git@martinmelin.com>
Matt Davis <matteius@gmail.com>
Matthew Duggan <mgithub@guarana.org>
Matthew J Morrison <mattj.morrison@gmail.com>
Matthew Miller <matthewgarrettmiller@gmail.com>
Matthew Schinckel <matt@schinckel.net>
mattlong <matt@crocodoc.com>
Matt Long <matt@crocodoc.com>
Matt Robenolt <matt@ydekproductions.com>
Matt Robenolt <m@robenolt.com>
Matt Williamson <dawsdesign@gmail.com>
Matt Williamson <matt@appdelegateinc.com>
Matt Wise <matt@nextdoor.com>
Matt Woodyard <matt@mattwoodyard.com>
Mauro Rocco <fireantology@gmail.com>
Maxim Bodyansky <maxim@viking.(none)>
Maxime Beauchemin <maxime.beauchemin@apache.org>
Maxime Vdb <mvergerdelbove@work4labs.com>
Mayflower <fucongwang@gmail.com>
mbacho <mbacho@users.noreply.github.com>
mher <mher.movsisyan@gmail.com>
Mher Movsisyan <mher.movsisyan@gmail.com>
Michael Aquilina <michaelaquilina@gmail.com>
Michael Duane Mooring <mikeumus@gmail.com>
Michael Elsdoerfer michael@elsdoerfer.com <michael@puppetmaster.(none)>
Michael Elsdorfer <michael@elsdoerfer.com>
Michael Elsdörfer <michael@elsdoerfer.com>
Michael Fladischer <FladischerMichael@fladi.at>
Michael Floering <michaelfloering@gmail.com>
Michael Howitz <mh@gocept.com>
michael <michael@giver.dpool.org>
Michael <michael-k@users.noreply.github.com>
michael <michael@puppetmaster.(none)>
Michael Peake <michaeljpeake@icloud.com>
Michael Permana <michael@origamilogic.com>
Michael Permana <mpermana@hotmail.com>
Michael Robellard <mikerobellard@onshift.com>
Michael Robellard <mrobellard@onshift.com>
Michal Kuffa <beezz@users.noreply.github.com>
Miguel Hernandez Martos <enlavin@gmail.com>
Mike Attwood <mike@cybersponse.com>
Mike Chen <yi.chen.it@gmail.com>
Mike Helmick <michaelhelmick@users.noreply.github.com>
mikemccabe <mike@mcca.be>
Mikhail Gusarov <dottedmag@dottedmag.net>
Mikhail Korobov <kmike84@gmail.com>
Mikołaj <mikolevy1@gmail.com>
Milen Pavlov <milen.pavlov@gmail.com>
Misha Wolfson <myw@users.noreply.github.com>
Mitar <mitar.github@tnode.com>
Mitar <mitar@tnode.com>
Mitchel Humpherys <mitch.special@gmail.com>
mklauber <matt+github@mklauber.com>
mlissner <mlissner@michaeljaylissner.com>
monkut <nafein@hotmail.com>
Morgan Doocy <morgan@doocy.net>
Morris Tweed <tweed.morris@gmail.com>
Morton Fox <github@qslw.com>
Môshe van der Sterre <me@moshe.nl>
Moussa Taifi <moutai10@gmail.com>
mozillazg <opensource.mozillazg@gmail.com>
mpavlov <milen.pavlov@gmail.com>
mperice <mperice@users.noreply.github.com>
mrmmm <mohammad.almeer@gmail.com>
Muneyuki Noguchi <nogu.dev@gmail.com>
m-vdb <mvergerdelbove@work4labs.com>
nadad <nadad6@gmail.com>
Nathaniel Varona <nathaniel.varona@gmail.com>
Nathan Van Gheem <vangheem@gmail.com>
Nat Williams <nat.williams@gmail.com>
Neil Chintomby <mace033@gmail.com>
Neil Chintomby <neil@mochimedia.com>
Nicholas Pilon <npilon@gmail.com>
nicholsonjf <nicholsonjf@gmail.com>
Nick Eaket <4418194+neaket360pi@users.noreply.github.com>
Nick Johnson <njohnson@limcollective.com>
Nicolas Mota <nicolas_mota@live.com>
nicolasunravel <nicolas@unravel.ie>
Niklas Aldergren <niklas@aldergren.com>
Noah Kantrowitz <noah@coderanger.net>
Noel Remy <mocramis@gmail.com>
NoKriK <nokrik@nokrik.net>
Norman Richards <orb@nostacktrace.com>
NotSqrt <notsqrt@gmail.com>
nott <reg@nott.cc>
ocean1 <ocean1@users.noreply.github.com>
ocean1 <ocean_ieee@yahoo.it>
ocean1 <ocean.kuzuri@gmail.com>
OddBloke <daniel.watkins@glassesdirect.com>
Oleg Anashkin <oleg.anashkin@gmail.com>
Olivier Aubert <contact@olivieraubert.net>
Omar Khan <omar@omarkhan.me>
Omer Katz <omer.drow@gmail.com>
Omer Korner <omerkorner@gmail.com>
orarbel <orarbel@gmail.com>
orf <tom@tomforb.es>
Ori Hoch <ori@uumpa.com>
outself <yura.nevsky@gmail.com>
Pablo Marti <pmargam@gmail.com>
pachewise <pachewise@users.noreply.github.com>
partizan <serg.partizan@gmail.com>
Pär Wieslander <wieslander@gmail.com>
Patrick Altman <paltman@gmail.com>
Patrick Cloke <clokep@users.noreply.github.com>
Patrick <paltman@gmail.com>
Patrick Stegmann <code@patrick-stegmann.de>
Patrick Stegmann <wonderb0lt@users.noreply.github.com>
Patrick Zhang <patdujour@gmail.com>
Paul English <paul@onfrst.com>
Paul Jensen <pjensen@interactdirect.com>
Paul Kilgo <pkilgo@clemson.edu>
Paul McMillan <paul.mcmillan@nebula.com>
Paul McMillan <Paul@McMillan.ws>
Paulo <PauloPeres@users.noreply.github.com>
Paul Pearce <pearce@cs.berkeley.edu>
Pavel Savchenko <pavel@modlinltd.com>
Pavlo Kapyshin <i@93z.org>
pegler <pegler@gmail.com>
Pepijn de Vos <pepijndevos@gmail.com>
Peter Bittner <django@bittner.it>
Peter Brook <peter.d.brook@gmail.com>
Philip Garnero <philip.garnero@corp.ovh.com>
Pierre Fersing <pierref@pierref.org>
Piotr Maślanka <piotr.maslanka@henrietta.com.pl>
Piotr Sikora <piotr.sikora@frickle.com>
PMickael <exploze@gmail.com>
PMickael <mickael.penhard@gmail.com>
Polina Giralt <polina.giralt@gmail.com>
precious <vs.kulaga@gmail.com>
Preston Moore <prestonkmoore@gmail.com>
Primož Kerin <kerin.primoz@gmail.com>
Pysaoke <pysaoke@gmail.com>
Rachel Johnson <racheljohnson457@gmail.com>
Rachel Willmer <rachel@willmer.org>
raducc <raducc@users.noreply.github.com>
Raf Geens <rafgeens@gmail.com>
Raghuram Srinivasan <raghu@set.tv>
Raphaël Riel <raphael.riel@gmail.com>
Raphaël Slinckx <rslinckx@gmail.com>
Régis B <github@behmo.com>
Remigiusz Modrzejewski <lrem@maxnet.org.pl>
Rémi Marenco <remi.marenco@gmail.com>
rfkrocktk <rfkrocktk@gmail.com>
Rick van Hattem <rick.van.hattem@fawo.nl>
Rick Wargo <rickwargo@users.noreply.github.com>
Rico Moorman <rico.moorman@gmail.com>
Rik <gitaarik@gmail.com>
Rinat Shigapov <rinatshigapov@gmail.com>
Riyad Parvez <social.riyad@gmail.com>
rlotun <rlotun@gmail.com>
rnoel <rnoel@ltutech.com>
Robert Knight <robertknight@gmail.com>
Roberto Gaiser <gaiser@geekbunker.org>
roderick <mail@roderick.de>
Rodolphe Quiedeville <rodolphe@quiedeville.org>
Roger Hu <rhu@hearsaycorp.com>
Roger Hu <roger.hu@gmail.com>
Roman Imankulov <roman@netangels.ru>
Roman Sichny <roman@sichnyi.com>
Romuald Brunet <romuald@gandi.net>
Ronan Amicel <ronan.amicel@gmail.com>
Ross Deane <ross.deane@gmail.com>
Ross Lawley <ross.lawley@gmail.com>
Ross Patterson <me@rpatterson.net>
Ross <ross@duedil.com>
Rudy Attias <rudy.attias@gmail.com>
rumyana neykova <rumi.neykova@gmail.com>
Rumyana Neykova <rumi.neykova@gmail.com>
Rune Halvorsen <runefh@gmail.com>
Rune Halvorsen <runeh@vorkosigan.(none)>
runeh <runeh@vorkosigan.(none)>
Russell Keith-Magee <russell@keith-magee.com>
Ryan Guest <ryanguest@gmail.com>
Ryan Hiebert <ryan@ryanhiebert.com>
Ryan Kelly <rkelly@truveris.com>
Ryan Luckie <rtluckie@gmail.com>
Ryan Petrello <lists@ryanpetrello.com>
Ryan P. Kelly <rpkelly@cpan.org>
Ryan P Kilby <rpkilby@ncsu.edu>
Salvatore Rinchiera <srinchiera@college.harvard.edu>
Sam Cooke <sam@mixcloud.com>
samjy <sam+git@samjy.com>
Sammie S. Taunton <diemuzi@gmail.com>
Samuel Dion-Girardeau <samueldg@users.noreply.github.com>
Samuel Dion-Girardeau <samuel.diongirardeau@gmail.com>
Samuel GIFFARD <samuel@giffard.co>
Scott Cooper <scttcper@gmail.com>
screeley <screeley@screeley-laptop.(none)>
sdcooke <sam@mixcloud.com>
Sean O'Connor <sean@seanoc.com>
Sean Wang <seanw@patreon.com>
Sebastian Kalinowski <sebastian@kalinowski.eu>
Sébastien Fievet <zyegfryed@gmail.com>
Seong Won Mun <longfinfunnel@gmail.com>
Sergey Fursov <GeyseR85@gmail.com>
Sergey Tikhonov <zimbler@gmail.com>
Sergi Almacellas Abellana <sergi@koolpi.com>
Sergio Fernandez <ElAutoestopista@users.noreply.github.com>
Seungha Kim <seungha.dev@gmail.com>
shalev67 <shalev67@gmail.com>
Shitikanth <golu3990@gmail.com>
Silas Sewell <silas@sewell.org>
Simon Charette <charette.s@gmail.com>
Simon Engledew <simon@engledew.com>
Simon Josi <simon.josi@atizo.com>
Simon Legner <Simon.Legner@gmail.com>
Simon Peeters <peeters.simon@gmail.com>
Simon Schmidt <schmidt.simon@gmail.com>
skovorodkin <sergey@skovorodkin.com>
Slam <3lnc.slam@gmail.com>
Smirl <smirlie@googlemail.com>
squfrans <frans@squla.com>
Srinivas Garlapati <srinivasa.b.garlapati@gmail.com>
Stas Rudakou <stas@garage22.net>
Static <staticfox@staticfox.net>
Steeve Morin <steeve.morin@gmail.com>
Stefan hr Berder <stefan.berder@ledapei.com>
Stefan Kjartansson <esteban.supreme@gmail.com>
Steffen Allner <sa@gocept.com>
Stephen Weber <mordel@gmail.com>
Steven Johns <duoi@users.noreply.github.com>
Steven Parker <voodoonofx@gmail.com>
Steven <rh0dium@users.noreply.github.com>
Steven Sklar <steve@predata.com>
Steven Skoczen <steven@aquameta.com>
Steven Skoczen <steven@quantumimagery.com>
Steve Peak <steve@stevepeak.net>
stipa <stipa@debian.local.local>
sukrit007 <sukrit007@gmail.com>
Sukrit Khera <sukrit007@gmail.com>
Sundar Raman <cybertoast@gmail.com>
sunfinite <sunfinite@gmail.com>
sww <sww@users.noreply.github.com>
Tadej Janež <tadej.janez@tadej.hicsalta.si>
Taha Jahangir <mtjahangir@gmail.com>
Takeshi Kanemoto <tak.kanemoto@gmail.com>
TakesxiSximada <takesxi.sximada@gmail.com>
Tamer Sherif <tamer.sherif@flyingelephantlab.com>
Tao Qingyun <845767657@qq.com>
Tarun Bhardwaj <mailme@tarunbhardwaj.com>
Tayfun Sen <tayfun.sen@markafoni.com>
Tayfun Sen <tayfun.sen@skyscanner.net>
Tayfun Sen <totayfun@gmail.com>
tayfun <tayfun.sen@markafoni.com>
Taylor C. Richberger <taywee@gmx.com>
taylornelson <taylor@sourcedna.com>
Theodore Dubois <tbodt@users.noreply.github.com>
Theo Spears <github@theos.me.uk>
Thierry RAMORASOAVINA <thierry.ramorasoavina@orange.com>
Thijs Triemstra <info@collab.nl>
Thomas French <thomas@sandtable.com>
Thomas Grainger <tagrain@gmail.com>
Thomas Johansson <prencher@prencher.dk>
Thomas Meson <zllak@hycik.org>
Thomas Minor <sxeraverx@gmail.com>
Thomas Wright <tom.tdw@gmail.com>
Timo Sugliani <timo.sugliani@gmail.com>
Timo Sugliani <tsugliani@tsugliani-desktop.(none)>
Titusz <tp@py7.de>
tnir <tnir@users.noreply.github.com>
Tobias Kunze <rixx@cutebit.de>
Tocho Tochev <tocho@tochev.net>
Tomas Machalek <tomas.machalek@gmail.com>
Tomasz Święcicki <tomislater@gmail.com>
Tom 'Biwaa' Riat <riat.tom@gmail.com>
Tomek Święcicki <tomislater@gmail.com>
Tom S <scytale@gmail.com>
tothegump <tothegump@gmail.com>
Travis Swicegood <development@domain51.com>
Travis Swicegood <travis@domain51.com>
Travis <treeder@gmail.com>
Trevor Skaggs <skaggs.trevor@gmail.com>
Ujjwal Ojha <ojhaujjwal@users.noreply.github.com>
unknown <Jonatan@.(none)>
Valentyn Klindukh <vklindukh@cogniance.com>
Viktor Holmqvist <viktorholmqvist@gmail.com>
Vincent Barbaresi <vbarbaresi@users.noreply.github.com>
Vincent Driessen <vincent@datafox.nl>
Vinod Chandru <vinod.chandru@gmail.com>
Viraj <vnavkal0@gmail.com>
Vitaly Babiy <vbabiy86@gmail.com>
Vitaly <olevinsky.v.s@gmail.com>
Vivek Anand <vivekanand1101@users.noreply.github.com>
Vlad <frolvlad@gmail.com>
Vladimir Gorbunov <vsg@suburban.me>
Vladimir Kryachko <v.kryachko@gmail.com>
Vladimir Rutsky <iamironbob@gmail.com>
Vladislav Stepanov <8uk.8ak@gmail.com>
Vsevolod <Vsevolod@zojax.com>
Wes Turner <wes.turner@gmail.com>
wes <wes@policystat.com>
Wes Winham <winhamwr@gmail.com>
w- <github@wangsanata.com>
whendrik <whendrik@gmail.com>
Wido den Hollander <wido@widodh.nl>
Wieland Hoffmann <mineo@users.noreply.github.com>
Wiliam Souza <wiliamsouza83@gmail.com>
Wil Langford <wil.langford+github@gmail.com>
William King <willtrking@gmail.com>
Will <paradox41@users.noreply.github.com>
Will Thompson <will@willthompson.co.uk>
winhamwr <winhamwr@gmail.com>
Wojciech Żywno <w.zywno@gmail.com>
W. Trevor King <wking@tremily.us>
wyc <wayne@neverfear.org>
wyc <wyc@fastmail.fm>
xando <sebastian.pawlus@gmail.com>
Xavier Damman <xdamman@gmail.com>
Xavier Hardy <xavierhardy@users.noreply.github.com>
Xavier Ordoquy <xordoquy@linovia.com>
xin li <xin.shli@ele.me>
xray7224 <xray7224@googlemail.com>
y0ngdi <36658095+y0ngdi@users.noreply.github.com>
Yan Kalchevskiy <yan.kalchevskiy@gmail.com>
Yohann Rebattu <yohann@rebattu.fr>
Yoichi NAKAYAMA <yoichi.nakayama@gmail.com>
Yuhannaa <yuhannaa@gmail.com>
YuLun Shih <shih@yulun.me>
Yury V. Zaytsev <yury@shurup.com>
Yuval Greenfield <ubershmekel@gmail.com>
Zach Smith <zmsmith27@gmail.com>
Zhang Chi <clvrobj@gmail.com>
Zhaorong Ma <mazhaorong@gmail.com>
Zoran Pavlovic <xcepticzoki@gmail.com>
ztlpn <mvzp10@gmail.com>
何翔宇(Sean Ho) <h1x2y3awalm@gmail.com>
許邱翔 <wdv4758h@gmail.com>

.. note::

    This wall was automatically generated from git history,
    so sadly it doesn't not include the people who help with more important
    things like answering mailing-list questions.


.. _v420-important:

Important Notes
===============

Supported Python Versions
-------------------------

The supported Python Versions are:

- CPython 2.7
- CPython 3.4
- CPython 3.5
- CPython 3.6
- PyPy 5.8 (``pypy2``)

.. _v420-news:

News
====

Result Backends
---------------

New Redis Sentinel Results Backend
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Redis Sentinel provides high availability for Redis.
A new result backend supporting it was added.

Cassandra Results Backend
~~~~~~~~~~~~~~~~~~~~~~~~~

A new `cassandra_options` configuration option was introduced in order to configure
the cassandra client.

See :ref:`conf-cassandra-result-backend` for more information.

DynamoDB Results Backend
~~~~~~~~~~~~~~~~~~~~~~~~

A new `dynamodb_endpoint_url` configuration option was introduced in order
to point the result backend to a local endpoint during development or testing.

See :ref:`conf-dynamodb-result-backend` for more information.

Python 2/3 Compatibility Fixes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Both the CouchDB and the Consul result backends accepted byte strings without decoding them to Unicode first.
This is now no longer the case.

Canvas
------

Multiple bugs were resolved resulting in a much smoother experience when using Canvas.

Tasks
-----

Bound Tasks as Error Callbacks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We fixed a regression that occurred when bound tasks are used as error callbacks.
This used to work in Celery 3.x but raised an exception in 4.x until this release.

In both 4.0 and 4.1 the following code wouldn't work:

.. code-block:: python

  @app.task(name="raise_exception", bind=True)
  def raise_exception(self):
      raise Exception("Bad things happened")


  @app.task(name="handle_task_exception", bind=True)
  def handle_task_exception(self):
      print("Exception detected")

  subtask = raise_exception.subtask()

  subtask.apply_async(link_error=handle_task_exception.s())

Task Representation
~~~~~~~~~~~~~~~~~~~

- Shadowing task names now works as expected.
  The shadowed name is properly presented in flower, the logs and the traces.
- `argsrepr` and `kwargsrepr` were previously not used even if specified.
  They now work as expected. See :ref:`task-hiding-sensitive-information` for more information.

Custom Requests
~~~~~~~~~~~~~~~

We now allow tasks to use custom `request <celery.worker.request.Request>`:class: classes
for custom task classes.

See :ref:`task-requests-and-custom-requests` for more information.

Retries with Exponential Backoff
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Retries can now be performed with exponential backoffs to avoid overwhelming
external services with requests.

See :ref:`task-autoretry` for more information.

Sphinx Extension
----------------

Tasks were supposed to be automatically documented when using Sphinx's Autodoc was used.
The code that would have allowed automatic documentation had a few bugs which are now fixed.

Also, The extension is now documented properly. See :ref:`sphinx` for more information.
