# Copyright (c) 2015- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, io, sys, time, traceback, shutil
import json as jsonlib
import uuid as uuidlib
from threading import Thread
from Queue import Queue
from socket import *
import fcntl, errno
import hashlib
import subprocess

from class_mesg import Mesg
from class_file import File
from class_call import Call
from class_exec import Exec
from class_envi import Envi
from parser import Parser

from json import JSONEncoder
from uuid import UUID
JSONEncoder_olddefault = JSONEncoder.default
def JSONEncoder_newdefault( self, o ):
	if isinstance( o, UUID ): return str( o )
	return JSONEncoder_olddefault( self, o )
JSONEncoder.default = JSONEncoder_newdefault



class Connect:
	role = 'Client'
	debug = False
	ready = False
	nil = 'da39a3ee5e6b4b0d3255bfef95601890afd80709'



	def demo_census( self ):
		#python_env = self.env_add( engine='targz', folders2include=['python2.7'] )
		#jelly_fish_env = self.env_add( engine='targz', folders2include=['jellyfish'] )
		#print jelly_fish_env
		jelly_fish_env = '39b6f01cbc9cdb36384fcbf28e1632062910cbd4'
		python_env = '0bf38151de392da4c34b97dcbef6ec8395a2f36b'
		
		years = [1850,1860,1870,1880,1890,1900,1910,1920,1930,1940]

		
		waypoints1870 = None
		original_data_keys = {}
		'''
		# Add the original data files in the following folder
		folder = '/data/pivie/census_data.familysearch/'
		f = []
		for (dirpath, dirnames, filenames) in os.walk(folder):
			f.extend(filenames)
			break
		f.sort()

		for year in years:
			original_data_keys[year] = []
		for fname in f:
			# problem with 78ded153133bf60abe73636c21628e1cf571aac8 ?
			year = int(fname[0:4])
			print folder+fname, ':',year
			original_file_id = self.file_add( folder+fname )
			if fname == '1870_Way.7z':
				waypoints1870 = original_file_id
			else:
				original_data_keys[year].append( original_file_id )

		for year in years:
			print 'original_data_keys[%i] = %s' % (year, original_data_keys[year])
			if year == 1870:
				print 'waypoints1870 = \'%s\'' % waypoints1870
		'''

		'''
		# Using these keys avoids having to checksum the original files
		original_data_keys[1850] = ['2f75ad1b96b0bddac41a0fbb476411f27f657e31', '1788a0ef544dc97de4df86c3d2a986ab45cb279c', '9632976d514ee7e3f4be564514396e79a0cba734', '11ff4c358fef805a1326505c23e226bf2261e65e', '9c301b1243650639d7f66d20a2a12630418b3b48']
		original_data_keys[1860] = ['f5f4eaa770388dc089b6d004aa4b61c7877bcbe6', '27b785b79d6448c1906892e20e0474ab0f6b8eaa', 'f4359ca23f87418fce01d5211d5aa334b5978218', '54b3c15585ddff32783e4c096b5d92eb0e39a30f', 'adedad08da85cbb65a2b1998e76807f76bbed931']
		original_data_keys[1870] = ['ba9cd2b8f2bb4c1cc4f40394485d0d5eb9294f88', '1e27bf234a8fcef131949d04bbe52a07c7f2f9d9', '9493a25d2bb6848e747f9b77a5826508189eead5', '178d214bb575fe04fa1f3d75e29cfde0e79a9c41', 'e88ea2d3a76896ab83de2d8d2d05408540f19c66', 'd1873fd2d4604ac311de54cf6a68021ade2851de', '726a3f5c292d4e42626ad9a423a911c86c04878e']
		waypoints1870 = '8fbcb207ca9b832ad1735ae8983d8eaa2dc9890d'
		original_data_keys[1880] = ['785173c3947fd00efdced8b2de2670e1ab493ecb', 'a75a0f11f8b447299ce0aa27fdac836b613e3880', '1f852cb6547d7e5b99ecad8469635ca6f69e621a', '29d06862eded4260706ca7b65cb1fc4472b68deb', '567f70ab2a40806381502812c7af813352385564', '7dd761c3b3dd8781a2cea3b1282fe7aa775190e7', '332ba905fe98e1d5bb5542fda40716f2dd8121f6', 'c605db195e9a7c0565be8a1ff1f7b736d0eba4dd', 'f23d2347f8de91b59e34ba085425801127f13f31', '95ef109b3aad8cd296654e7637d2c29e475a6577', '2f9152809039b88965e517baa189126cc6f514e4', '41c43f33e436a97bda505758b1282e91d04cdb90']
		original_data_keys[1890] = ['1b6dde6788a6c2925a3dac61e42be32107590b9b']
		original_data_keys[1900] = ['f69b7e8f593eaf046a64230153aa1ec63b3e32ac', 'a92cc06a768262f8d21aec190653dfe9d0df13c0', '652f1f46acc5d788b323a3dde1f752ac48986368', '7d642a9cf205a498453210f7ad6343863cf9ea98', 'b0299a113c28f1811f440b5d6924aa52f918f3c6', '40b30fae53a0879e07ca4cdf27c2cc0d8dc77a23', '1bdb68d83bb530788aa766714b40484ad7777125', '5a0c74ba3616307350a2366de711404c8bcf48da', '2a0b8eab884582c224938faae12ac034cc6c7e04', '61e6a8083209a06c1babbc74d3ac85e0ad9bcc7b', '952b0626f9bc85a98feee64ff5db0d0aeefc1674', '2b5cfc209a59ca599fbd947a5a6f45368672f87a', '4e4e114e23cf0d0287d0b83f9f0f8f2bb2832dbf', '41c4fd6a6cb375beef403da3c4a274e361e44e8c', 'b5a38e42c23858a9f6e480da33ecf0e5c28770ad', '1b353145671a3834d16286f4d140238c9b3b15c0', '9af30402efab004f3f2aa5c2ddcd928e55686b06', '61867de9921f2176865f7160c26706a4ef4ae3a3', 'a416d61c17f99b41962b9e16df07f292d37630fe', '0eb320a9f7d81dbfa4de24f57c9645bd240b194f', '8dbc5e8d2672b48be440ce998aa9e17f564ea2dc', '2add3e77b93af23bf124c6cf0d0d4a5fed2c0c06']
		original_data_keys[1910] = ['4d61f9637baf5d84bad5dcdeadac2e34f9ef32ce', '6da3cf8c2b83e5814e117e8d2421eb65db88a7ba', '4b333ce4089f5c7b9efdac5feb51fbf8b03f75ae', '700cf90a3849ac780c669209b7a3c3e1fb028f25', '0d716ff298a5d44c4d5e0ae5d030526cf2c85449', '81a08128dd2dcc061cc8fe0c2d78d1406dd31352', 'c2d496270db634775f3251b090cd6e1fa1a8da66', '53700f7389e5919b777af16d1d19f098b7651ae0', '635881eeab3b2792aa53c887dd4050e54b77a6a1', 'e8baa2258b655a54d9ced173bf289494b9c29969', 'f43fdb0ef6b28ea5c49608f67b05f71adabcf3ba', '1afde91475cb338fa791cdef7aaa5210e61b65bf', '8f085be9fbc954974858874db6c29858ab015e0b', 'ffac1841aa31ab017b5623a117b994c7b14a5ff0', 'c8821f672b67d5750ae0003b0755b90f559c1546', '53fb1db9665c78db22cdcb0cd7a41f05f0028bba', '05c4cd9e1d3c101cca3bb6b6f0475bfe5e36a8d1', '7198ce85c38b7b5fa8bfa831905e1e214d97a00a', '4dd2d6a092ac7d22736e1c4fec53951b6bd00bb3', '4e14319a6b261782be47371030dfa0d6f686d036', 'ea53579b8277b200e52999875f50915590d362db', 'e1ead7a0da48e93550dd29db53eea6dd656e2b39']
		original_data_keys[1920] = ['fb49cdd232432e8a03e5a4dc4f5bc92afdac3e94', 'acad2eefd4c3e532ad682d1d793053abc57f0e8f', '4bab86eef063afb9fdd123a72b1efcea32ac1523', '3dc6e950178c2d64a97b4a4e8231cacef94ea795', '04f342a3cde082647952e46cfc62f5726851834b', '60bf2044a1f14196b0e204190d9d801b73a9486f', '6211b8a80e66952db45227cfe797d71cef634c25', 'bb711830f2e79c034b03d76fdf9ac037297166b4', '96be5966bcd09f5014382c5746cd38e02193681d', '14601e86640909f2cbcc31c0b71d2c2615b73392', '6f58b3028e9783646285976e3d63d66281e96eb6', '37a74bc3c53a70e83a07a692aae4fb7d851be613', '1802075910f7b8296ecd9b4c31d3621f05d5f3c5', '818620a085d19ac8d37121af0b038debdd727ca4', 'd35e0ebb6ce84b1f4199d175e409c68988f97fe1', '9e8948e3dcce906654aeb412ab7317b9bbe1c01d', '6f9db5457e524c63a0ce215caafcc2858be7232d', '13dc7c7ff083ec81102402f1e079e9b0f53dec72', '894ce2358f6f492aabf278ace0d67797bf733d87', '10635106afd438406a9d59758cf0fba67d591389', 'cf134f15d5d150e1225678acd8d419a16dddf303', 'abf3ed0d70f108b74805af3a47831a0bb58f4f6c', 'dfba5344d71146eb4ac525cbe4d042e0e1e0dede', 'df402c7506e373be42b8e82c3ab88a3bff607dd8', '3257a1b18c78835bf5bab5b0468758483d668989', '70833dea236bfcd820c1bfcb61c694f74fcc6f5f']
		original_data_keys[1930] = ['10475652a3bdacd669149a5137474fa442d8f34a', 'a5fa86ad22db11a80937c807074424b81459c924', 'ca907bbac9044d074ddb9c1d4d217c743a2903f8', '25031517d1fea413a6afb480b6703e0541485cb7', '7df89f8550fe5465c6b762429bbe16a542091dfa', '94941670dc5140926bf47a7ac6a0c76a466f20a4', '1b70148edbd350983b895c5d67e16c60b4932b35', '78ded153133bf60abe73636c21628e1cf571aac8', 'b660fc5dc8bbf7d89b67c52be95eb1646580cefb', '3011e1737796743e8ce19204c0d7e51e1d6a9845', 'b299d385b6de52cabad501e10a04157df3bae8a4', '07412f07b0ca2a932329457448685263cd21ebf7', '53d50e27c8621ee5bdb498f97fde2db969f7d34d', 'da066aa33f6cca3376d28e6bf140bf6a223dc64b', 'c95c725a9ffbab8130d273c4492ea536959a53a7', 'c6f8e9185cace5979a0b3b70dd7447b66d8d4029', '031f76ffab09be7419913fc48d8b8aa2a758ef6a', '4a6516221d05305281a406c23159a581205e1bce', 'ecf93ed1cf4f1fa862e9bdad1896428c17494d03', 'c55634da235811d86a3c5173eb8624e77b5b1f19', 'bfa846e93a57ce7fcd5c5758ce8db8bccddb27c1', '46c892fc0af8a4922bda99cb0bfd4a056e488ad6', 'ade99a9cbc5ac5b96b8f5d418db969d487b9924e', 'd108013e705094cf3658b4a8dfa281e71f5fc94e', 'aed4faefaf6280b344f240cf6b704f34184a268a', '4b3604eef53f3150602fc79b710f12b713a4ec83', '41df1e1b30ed957c392fe7da03e1ba21eaa39523', 'c07ada69db1acf9cd9e6a31db84dc75a0e36a6e3', 'c6e2e58ea1f98a1fa421b180dbeb1eda1d595a13', 'dd632dc3cc6907e19655b1c2c16621aa807d8b17']
		original_data_keys[1940] = ['287b932eb33c21270500951ef5719f25fd1021e2', '926534093365ca481e9c6e498d9d666593b119d9', 'f0468cf894eace9064f384ad577e28b55fb831dc', '0fe8861905e36ec5c3f92509e9ca7296545d28d0', '1e4106311f7801f26ac73d0b7b122787459485bd', '3e80042d7e0f5fb607db8ca60bc523c2f72f3f6b', '1806f9dfec462dd9c975b388473c27cea9884a63', '4ab18fa9e2ea9a012f229f2c94d0c74b4073647e', '9a64e1a4d7eb6ac3fb3414f1f23d0c283c86762a', '5f0a135c4c6b1458f005455ebb1b9543ea71b0fa', '8b0c670913eb3a068ac92f96de64f0d5820ee0a7', 'eada25d6c755a21fe0897429f081971d14a1110f', '52426e90a5efa08c79e7b235bf33cba6622017e0', '5123640fb85f0364d7864b45b02f87e59972970b', 'c2d125b714608e2feb364a8e21f408b880d79dc0', '08cbca5dc0626664cd87e693f84a6c9cf6b505b0', '0b6cccc31237e0219a920e3dff99d32fccd0027e', '803ea61f4e981795335673292c4eaa06f89676a7', '742a1736fff42d73e5441f61d11ca7cb3ecf5a59', '0598690cd4d09015623b36a8484223acad68bf52', 'c3fba341295dd41ee2201aba3891171f5846c0be', '60e0d7898b5323c5b7696305c99cab6bf1ec5044', '2e54262020fe76013415a334160c62915cd02dab', '59e6f3fa106725f30dc87c529c9cd76e1290b3d8', 'a8f2453418b080f798d512af7a290075ab1c4062', 'dcee14d8b53c9e1b8dc7ffcfc482862394f5ece2', '78ecc854fb76df08a33323ca805e66522a8c3b15', 'dca9698b18616de8f5009df203ffb79b449daa25', '8849b233e56b3bdbe4d6c789360ef5d572e3b43f', 'e76aa0b190ac8e68649c62af7e439373708a21e4', '6168a6573338c5c38fc1ef682c2ddb27654c5154', 'cc200ac9ea0eff6af4f9deaa322d8a9e0fe4bc7e', '5899363bc837acc75992cb63b7a78c7770615067', 'bd65a073558798e5c3bb2f2b95112ff2e56ccdaa', '2775cfa51b7494e116e7be92caec550847233b3c', '70f5c8c4a4b04772639d83262a95aaa1dadc7924', '85babb6f80fee0edaa27951713f404287d1dcdc2']
		# 167 files + waypoints_for_1870
		'''
		
		'''
		# Unzip the original data files
		for year in years:
			for k,zkey in enumerate(original_data_keys[year]):
				ukey, = self.call_add( returns=['output'],
					env=self.nil, cmd="7z e input.7z | grep Extracting | cut -d' ' -f2- | awk '{$1=$1};1' | xargs -I '{}' mv {} output",
					args=[zkey], params=['input.7z'] )
				original_data_keys[year][k] = ukey
		ukey, = self.call_add( returns=['output'],
			env=self.nil, cmd="7z e input.7z | grep Extracting | cut -d' ' -f2- | awk '{$1=$1};1' | xargs -I '{}' mv {} output",
			args=[waypoints1870], params=['input.7z'] )
		waypoints1870 = ukey

		for year in years:
			print 'original_data_keys[%i] = %s' % (year, original_data_keys[year])
			if year == 1870:
				print 'waypoints1870 = \'%s\'' % waypoints1870		
		'''
		
		
		original_data_keys[1850] = ['a0f3552649659384fc3c1c76107c0bbb175901390', '233b7bbf3dbf5eec3340d181abaa03ef26610dc40', '2a4d8ff77783f75c7ff6cf42d9bc21fa210fd7b50', '2b647382cb2da7ac6c01494693f123186078a3bc0', 'eb67044a94de8d1603888c22267fc5cf3c5ac51f0']
		original_data_keys[1860] = ['38439e1323d96b6c9ae3c61c3771645d1a2a258d0', 'e010e0b5023aa308b691783a1f3ce77ffacea7370', '120c7f7abb27254fb058e8fff75eaeca4119f7ed0', '24959c790d3620621cceb642d9893e22ba816e530', 'b6a3e419a9e4577f79d2bdee0820cd1e0a9be29c0']
		original_data_keys[1870] = ['96f8fe200c5a110dc565651167797b658611bf7c0', 'f32d92f8b72cfb71eb03df5ec36c33a3a78efde40', '45af2ac1718bf5d596068797a3fa3ea3aa9b60e20', '68131732904d1812910104c68bf23b3701613c940', '893431df417b9e64c0be650d0c80dbf127e510c70', 'd1912698152879ff220a1c65f8d61cc7985f7ce80', '78bb197c8a8b3f1eac9af65df35784aaa742944d0']
		waypoints1870 = 'c0e4d6236650c898feeb71032f71973ca2df7ea90'
		original_data_keys[1880] = ['bd5d13f5b816212608193cd9683af65c4303cb620', '02ab0541b24cba0e78d440c06bd407a6982037c40', '8d6e19b50484f9ff5e7580b63bc3b60841ac9b660', '819528e5179acaabf915dfb481556cc0c20957fa0', '41ea3531f1ea97a739a751e16dfded40229ff4730', 'a9ed4c2f68b74d3aeba38e945d96a29826c4d5520', '0e6a12c3e1b94e686ce0b0658fccec55cf69df660', 'e0667d413198a180caa3d1fc3a3586e2be17670b0', '6cc3c91b6ce984baca8041f3b0cb9f06c43d15830', '67e158a12a892a85a3b5b89e3513de9c97b31ead0', '55d72d0ae50d4f6c79126b0d93114e540c09a6160', 'fa2b7be7cc6229353672c22fa7bda0fbe3a0a8010']
		original_data_keys[1890] = ['4882fd152366e9aac4c32ee6d476e2ef3ae43b610']
		original_data_keys[1900] = ['8e2ca7db411f4c57ffac0c271d9c6da722a027900', '78b1af08b86158cdc4257892b30461545bcfdc580', 'e0eb6e377e54bae168dd6d2c532527f10dd366ff0', '04b0d6576a5b7613cb7a4ca37e5a27a40a4134420', 'c9e6bf8fef4363daa2ca4f5f99459d657cc92eb60', 'c91d633ac106a75c0adbde89cb4dbc8c8c54e90a0', '0ce0e9ba8f4a83637119fa74e747368a501cb80a0', 'f08d1f1b84b7cdbf74cee0667e47b078f82583350', '6c965a65ed203e84f2358efe7ada57d1779545ab0', 'af6426541e7579f81c9a6f304b3302d39bd8034b0', 'c92d19095180e32785f1c2a5d70e212581ba4baa0', '07bcc5c41ebb569e2392e71db9256504cf775b1e0', '8b9199a0a5dcfcd440db0909f47c011b1f23bffa0', 'a9bd432f375e6f82ee912937c8d33b64f43ce7e90', '7d43d968b469117f219b6583f5fa05bb936565a90', '67f6753fa266a9b9db19ab8b15b97acc1d6bbbb20', '592c4b26c723d80ef7cede31907e07eff1edc8040', '57dba82492c896e932790a9b92737fac6f691e190', 'd9ae3524a54b8c7fc09a5de56fb5395e566613aa0', '5c3ea2cba9d7f473880fac1dd7fd3ff170589cb60', '1037a807f24578056e66d61d69316d8772afde3a0', '98d2296f71cc82193600f20a5e5ae266e44058770']
		original_data_keys[1910] = ['4be016df3dcf12bb8c32c11522b877c09be294650', 'c0b3b62e858eba3009ecada5fed5b738ac90c3f20', '396c5c9ab90325a9618204970a2fb173a057b7a30', 'a1cb625eb8fa3c5f5c236dc0fd0ad14e472da5950', '810c8354240f11339fd03a08fc9c8d2e09cddd6a0', '41483fcfad9788706bc76ccdaeacdf3b1e8fd9530', 'b886415f1453473742fba8e25c111aba4b9206ff0', '57ea8e80c45fefa7128cfd9ddd2d48316933e61c0', '2905b5a7bb1d305f57c32141d975c74dd97cb5840', '0a6ca70f4fcb31621e0da67ba2a369190211e04b0', 'fae68eb9df6e7af8574d3cbc02a45e28e0cec1370', '6eda609a5941b9a24ef43a52fff34c761838d0f90', '1385adf5d1dcdf17f4f1b25bd67fa06ed100c2be0', 'b3597a91d3ff6f9e28b685cb82d6b93b4c779e280', '9e1f8f2657f63ebaf8ac22bde56ced089e1325610', 'd40e534c120d63581a10f3b38523dcbbaf83acf30', 'e0b37acb1ef771e92b48ae084489fba17405e47e0', '3f033be29e07128b2f3b9bc441c71feb4f0ed5360', '17ee0f7c2a0b31d36c38c8018c315d23c744614f0', '04e2f753d0a7be8c67a237bdba69d69ba2a223c80', 'ac1fafb48da80319d5659247f1faa96ec014f2810', 'b8db4d9726b32d6f5eae43ab91c0b7ebff610e140']
		original_data_keys[1920] = ['3413aba3194c77f219a5a3b4b2e498f02006e56d0', '39e37854ca8aabc3d7474c7111192d693662a60f0', 'a1169b358de93fca548e918c238ae7f6259dcb910', '4fdb2191ec58ea814cfb9d235db9ac113845168a0', 'a5ee4ff89011e144846dee7a3a064819608fcbfc0', '3c22c5f55cb6070cfbfaa8f9dfe0c13dd3550e1c0', 'c376e72b26e69f152a97cc88f8119e8aaa1fb32b0', '2f8cfc6e6e3edd8428af1f2530caad4eb3c0df750', '58bb74e8e8b9914d175bba0964561c3519a7c4a20', '84bed2f7b440c0a95cdacef986b6712975460a300', 'f6b4a6d1ee405b579002e324235e6f8905a4ed430', '93b3dd86a36c22c2f42dc6c13ebfabdd301012190', '46338ca1c67f1d08d20b6b3d10a7320c0d4f54da0', 'e386cc92b2cb464364ee0485bb0ba6907242a9d40', 'ff58634e79b23e583a793a8cb9506b339444c4d90', 'd7d4e97ebd9515e310c58245a6600982e86f04400', 'ec6ff5ada9df33898eae06fcdd74acc980e59d090', 'c0968b1f434864836397f1db6cdc80a1093231850', '362de2100acde9e0291e2784a03fe51ae9a7bba10', '8c00550909515685c199843f572ef422025884100', '7a33afc08f3381930fc7903691366bb582f124e80', '98dea63539491daf2af02b01673601046a1f39d40', '868a72f38a0632de6c5c43c07b4059aa3484213b0', '30f037b8b7242ada2721a4a41497255ee2e1dadd0', '82fa702afdf499c67ad161f1c2f75790408c0db20', '54f4e2a446969a7dcfed462c50a14065dd0b0dd20']
		original_data_keys[1930] = ['70646c715fea0ccb6d30cf9408b248cf3a0e17380', '6aacaafcd26794b38e462f33180004e6b65306910', '15b57cdc8aa70e79f52f3895c1688510078f5b290', '8ccd396189be5e795966d7f9cf11604ec3c58a4d0', '4c10007f53a626fdda91a29cfda58d5daf79bfc50', '9394ff1a7fa9d079432e8d4b68b6936f8e8dc1970', '879b88c67a18e57aaf691ba0770e7830941ac2fc0', '6daea38969344c65c6d7bdc6cac4f2b10b97519d0', 'b074707095875deb256e34c1de1b1a588212e5300', '8c25b39170bf2158055e3351dd532df79f8eea240', '387af93ddd606adf311c47633f34238c8ae0cfb00', '7020471c63b9fed8ae86b70de265a7a7985c71e30', '6e5256192226eb84331c24a9c5e9eca55df1dbf50', 'dab3c781afc74ad5313e703b183f83ae47a86ed40', '56ccf20754425f4a6ac7bf12861df93ff3ed43b50', '9f358f5f11fe346588e5fd8cef5674f9c31761ad0', 'd9e32d0f15906e7b26138dd92658fa933092e1b20', '8ad2494f0eb97fba74d006f98e54a9a43b16b7b80', '06a8720979f0fba07639aacafacbe02c30237c4b0', 'aec52767b606d96b335e59ecb0936a6a7ea896d10', 'c51845e6cd043f530fce970edca9de699a189d220', 'ff461ee7767774bfa57d03c69fa146c61daeadf90', '8b1fac8069666c3a0435e3e3e146bb791fc23f160', '303b9ef05193eeb84453e8d0b0e461a6f9fbfeed0', '55bb4559930dde99a59a434b0b9ed7b649dabf4a0', 'fdb81377d828118a6e37a9ecc6b407ad0686f4fd0', '76bf0025630be29873f484752c42f3b14f75a5100', '8e9ff06368259157b36b7e8457c30a93a492ffa90', 'a03741af1d6be917ee845e580323a799c5dc7dcd0', '83e909b1f8007b695c001a69f7e87d1fe62751f60']
		original_data_keys[1940] = ['48620d52ee924583c492db329e63a1b11ff44b620', '5ecd52c50a6a2ea38e20f074cdc917d2cd1d86c20', 'a3f0a92f05db45c29f19db4d0d7c62ed3266ce8d0', '0fed494b03529bb09196a8d99ffa84b8fdafb8b00', '474f64df36a4c9ca17d2466130045849e4ec1fa10', '1e7a179e9ebf442ea60dfd4c13044a3098f020fa0', 'dff854fce0cd7bbd501fe064a6686fc9a6c454540', '1dd6a070bb5a0ed21208bff7d97f44b9439b461f0', 'b1508275e42a3002718578d7bb84d5aa6298b6170', '74b863d8a0619a95babdc8e39e95f7670f7956730', 'f735cda7918187d72a22f202f02d67db7e1eacb60', 'b24a477d6191f598d01a3fbf8cc93f899a8e13280', 'b524af40b715b0a3be887c5a611fbe02a8913d9a0', 'cbb8e3c802aa067cbf111d1561fe58c75f0870010', '5a4c43c27d7221c5c43848589b86292bf2d8e94c0', '027f59ecc011e70039ae9ff6765bb27a73bd782f0', '68374c53f95935c3f30e4f61b974b21438a43dd10', 'a83834bedcbfc23798d79ba2fd3ce49629bba33e0', 'a9ca567819bfedff49c9b442f80e63a2d2f3309e0', '3b9de032c3e3ef4b806912b05b63d888b575ec890', 'cd99abc784979b0b508c8e475aec8730722936d90', '26af20a0755f3498456359fc7868f531170010ff0', 'd21f09f56847cecaf176e9f38cd75f0d0f152b150', '4e274ea82448b54ed0880d30b22e91017e874e9a0', '9baa1125b20a2f76a91c93cb68376e4c110183d30', '614e53ded5964419bf72c5e11e0356b08b9ad1a70', '9dadf5ce1cd3027e056210fd7f4571297689cbb70', '4025ff0063d68d8b4db167251a0299a5445a69c10', '1dba592e32aa980653d781e68abf6b6dab5b27a70', '2b2a0e491217db5c33abe55e2713bc1f0b71bcfc0', 'bb1f6b6f734cc74b56613bb6a6c657c2ba2cbf720', '5584732812a51e88c1854e9189ccdfa44656d9390', '20284e2d7b29d28dacd88392ac489b1ac9c289d90', 'def49f88f7a7fcb2f5ce6d0619b6429f7a05fbd00', '3623ea573b13122a35aacf4e14853d5200f8bec60', '977a53886a0bbf7add997c73c745210282fac1c20', '82a12acaf7428fc90335c187bd1dfeab64fca5b50']
		
		
		
		normalized_data_keys = {}
		for year in years:
			normalized_data_keys[year] = []
		'''
		# Normalize the data files
		folder = '/afs/crc.nd.edu/user/p/pivie/census/normalize/'
		normalizers = {}
		for year in years:
			normalizers[year] = self.file_add( '%snorm.%i' % (folder, year) )

			for u,ukey in enumerate(original_data_keys[year]):
				if year == 1870:
					cmd = "./norm.%i waypoints < input_data > output" % (year)
					nkey, = self.call_add( returns=['output'],
						env=self.nil, cmd=cmd,
						args=[normalizers[year], ukey, waypoints1870], params=['norm.%i'%year,'input_data','waypoints'] )
				else:
					cmd = "./norm.%i < input_data > output" % (year)
					nkey, = self.call_add( returns=['output'],
						env=self.nil, cmd=cmd,
						args=[normalizers[year], ukey], params=['norm.%i'%year,'input_data'] )
				normalized_data_keys[year].append( nkey )

		for year in years:
			print 'normalized_data_keys[%i] = %s' % (year, normalized_data_keys[year])
		'''
		
		normalized_data_keys[1850] = ['d90fe28d156cbf7a6958a4575c6e7f4eb9f045da0', 'd15cf24ecb1c32c70d195ac673fbd5edd60fae170', 'ddd3d04d727b3c835f744991dc62008d732d96f60', 'df7f7d008a6a61fc05a991bd5326f66324e669810', 'df2609f2e45f2ddf455b1c163cb9609b7a771e6b0']
		normalized_data_keys[1860] = ['2f6cf0e75c59722b7808f7324085427c369bcd1c0', '24e6ee048ecc065b4fe1eb321bb38b2a89b1dfdb0', 'ff28a4cd66ca7e2e4e8eb667953bf2caf60b4f940', '16cb005e65d4870e244c31baf6d1ba427d3323110', 'b652203daaf93647ddfd210bec866c5f5dd497580']
		normalized_data_keys[1870] = ['58efa7d217034ff0376cd21c482fb93bf4d0c40c0', 'bf2ae60c576ebccddd2097838951845e853cbc0f0', '7c9c4c76717b8a87bb832b242124fc52b119757f0', 'da00aeb259c32dffc0c6a1039f629cbcb82c27e60', 'dbc600e40a0993d8a51b51127864586f4a0a62ff0', '03afdd61ac9aa7e9a9b0c23ecc1ebc608d7204030', '4334ee782c61cad6c16bafd6f72b48df1550588f0']
		normalized_data_keys[1880] = ['774deb34ebfcb850583c7bc02a202189ef92b9930', 'e9edf39eb5d0a64c5bd43dfcb2530c6441376df70', 'b2763706065c49cb39f45066b180d405a464d3ad0', '35640b956a6cc64ea29f3acbf24e9e762e907f710', '99c57eeb26ee7b644acae9360bc3dde38c46330d0', '1114c7468d87673955ad85b88483575877d4c6ca0', '80fa607f2de7d678a64c16be132744e1a0f0e2a30', '313b7a7f7674bdf40ebc72f9d454b97aca165bc20', '0dc27d64b83dd60e2130fe979f7b1255ec05d4200', '812bbc951c129f7f63fe04122a7577fd7a37c8c00', 'c0289877dde9f5df5c0a4242306ecf5a2a32204e0', '1394095000bfbd0a1d35f47ec14004fbd7408e630']
		normalized_data_keys[1890] = ['ec96097818067f06b1bbc0d8d95d32e8783ed3a40']
		normalized_data_keys[1900] = ['ed5c4dd55b878b78366bb2760b7faf7fbca38c570', 'f2193bf9fd3eaeec73efb577fd4ebe12ec5d54b60', '5d04634371bf5091d32b1a0533ebdaec7a0dccbc0', '19c0b56d7ea926532d59ca98c708dddbd1acb9040', '056007b78d4a76dac0d4536f9d226565379392050', '4b5892ec0d940c179323f96ee90730c817bd27160', '85f143df5ab7c16e31cf254422a61cd0369fd7260', '6356f4dfa74028a36faeea4e69518ca955a94fcc0', '9741076362fb5d9c62ece45160dfb623a73983850', '0ea09bcf4795199c48796ae6a7f7974f9e3ef3bc0', '4512fd8de459248c6696b08d56de6965980d4f960', '8cd7356378cd7847d8365efccee1bc3e20a6547d0', '6c80ec4d8596861e2cdd766e329abf6491236a370', 'f6da8de0d9064f23c01cac43de5e6a8b987875e80', '616aa69d4fcee3634e0dab99adc9ea8aa6c6d81e0', 'efa38ffeffa82cece864d978bb0b45988cdc06110', '13701f5d895bf238170eda3926eb0d1982fbaeff0', '86d1817b26ddda72563bfb8b7e879b6b3e1810670', '30ed0d36bad8800e932ef8553543ddd36ca69d2e0', '0be55fe2d98c122ee077c705c57c879ddce673140', '0e02e9447580261a81c544a6de38fd6bb4bd95010', '8966976d5b382d3638bd4c299960e5685d545f9c0']
		normalized_data_keys[1910] = ['4903d4f068fef51b400d11c123cbe6b02a8926c60', '1ddc1947d3b551b810df77d98172dbdb9568c4b40', '5c4fc1441932a5f7140ccb615d85132a3cfbe4a10', '3714b844f5ac213e4ef9d39914e385439e77bf600', '23b6578b24942de512887727b51b5930ea581f0b0', '38feaea04c2ff32a540b720c8f9e1bb3878e0c110', '4197774ee35460fcb0c6ee8b8c4de5e7b3617b7b0', 'e7519ec8e24211a294b6eb39f3603badb2f944f00', 'a8e322ab6ffdb4a32edeece448b54dc8ea7c91f30', '17df79767a4188878bbb08b39bf90f95aa33a6e50', 'd1ca045e1daddfc03519265f2af488fbb32a1bdb0', 'c74cb6c744719d917ebb804d82e0ad9127a688ef0', '02c041f947e7f9ddb23989a4957acc07ac7b88b40', '5435597e2d32beb7216c6acf376d11ab16bcae990', '268ba3a4263b43b59809781c256f091d7b3dcc390', '7a3592f60f75b8f09d83d68c665444131fdb13d70', 'b01f7bb55a36110662f2cd5647a226009df34f190', '399f7409df7f471e62487877fec783e0b24c6bb30', '60a44a242646ac1ff4d35a0cedde9f8fb1df828c0', '4c5e5057c2d3da4234cf64905b2732e7dd3506ce0', 'f46582798ca4c49aa034a2e0c527c719930e32710', 'c6b21cf898172b32bb76d504bc1e2598f331c2420']
		normalized_data_keys[1920] = ['f3e6de9879123aa827ba648f3ee14cd7b768d6f80', '89259af113b7b21451964af7fc752970f722212c0', 'd15c58c9174fe206fdd783cea5c03c6f1f3372a40', '44faa08146623fa083fbb8fe730f7e97368598bd0', '3df996aad75181ef2d3b835d5a6c9c586b6d68ff0', '890a019002a7752c5b1904c5926a5b807aa026280', '0b6d1f95816e9b6822651d50f1deba96b05deed60', '4cf063eda87d81c12733aefe32f29d625b456cbb0', 'f05fa1f46cea80dc528fd762ef459d2af6e914370', '2830902d44eeebb47c03ef36b868a0a779a444be0', 'b1d999cadd1d6283f1601ccfc0188c8c529fa1e90', '39009b65d39293cf2fee6b6abf9846154268c3da0', 'e3d3a67e79e0fbfec61c510152c3990652485b7e0', '38d98ce0011049310025fe9121e16929780026ae0', '65612f7dfcd59c5525c6d38a9da611a8819e19590', '1cd9945a116c670ddf885fc39e7a564e18c4d59c0', 'a3b045880f1d291c64573818c2308d127987c15f0', 'aa86732a7f69bc452a34552657da284bd70a54b20', '47e727a229d9625f9e52262e815aa1feec901d970', '9c31ab4e42d11ab4ad9d3c5ef72b7421f466bc0b0', '1e0022c4fbd2db469cdff349e6e2366f2131ad1c0', '1f11d1efa834e6e7829d7aaeae6687731e823a230', 'b4397f98aac03592b04ef4652431fe166b85bf410', '101ebdafd6c45e3ef83e59bccdd21d0901df56cb0', '90074a3819abcb13c5e684c3a617add8a64197180', 'c10733fa02685023d5f84f7348af6d329979c0040']
		normalized_data_keys[1930] = ['9c13162e38f7c7395244fe6e6910f9181a5b7dd90', 'ead2e524018789f54014bec94cdc75b0636c1be20', '983496f5a4534144e24ceb7992b3f916f150cb450', '20a516eecf0486cf3b204d211f86f90d29d797340', '8db59fa86612ee9f172fb6a50ccd941925973e9d0', '9ae24faf0d00d70ab42cc0b73089dac1bcb3e07e0', 'f5bf7e2136a9fe92b5dbaea5986f2ec1d97624d30', '7c8412ce82c7eb50a8ab8bd34a5f40121e2e390a0', '867bce31bba7cd61fd2b16cf559778ba0a9c0ce60', '55f7a32615b53025d3f005c0732fe3895d1710a20', 'c4708aad7d2f418fd294e32fe4cd890304e61fa90', '1b78bd4b4f0c50139f9929e09cde5e3d9b3747d60', '7de36b4910ff1922e4caac605bc67d7b5ec2e0be0', 'c8bc5a74f587a1794310dd2ca78cbae18174d4440', '745ba4b8ac6b8c30fa2f63edd3b73fa76bd7ddc30', '0811b8ed118d85a93179659520cb66a38e8185fd0', 'eba000fad4acceab42564a95ce8fed52cd53ecc50', 'c77ac099b2dac97cde7ba3a6a140d6f11536447c0', '6e31281740cadab0bba98aac246714aba9e138010', '5de2bbaef0ad9c5ca65c2c700164a668bd8366e70', '8d3da413342634f08ab1fa4afc8ee942063764240', '364e28905c7b01bc751c8064cc7b2bec83c594210', 'c18df5f6cfa79f45c8f59c828c415dbc05674ad60', '93b78c9d5cdfe912fb0b5c2a74b354bdbad979bd0', 'e75150ad2469491de818aac1f4aa863bf179d4420', 'b1e3de8fe9d4f2a74dedc8c9004a0dc5d3be43470', 'bb4f606edf1f6a6451b8df0baeb70fbec5c10b3d0', '7da72c2048ee6268563dc2c70a3c20556f5e3d420', 'a3dcbb42149609d44005e34d9cb9efb77009b59e0', '6344863b692863e89ad547aa82127d694b498aa20']
		normalized_data_keys[1940] = ['2526d52c66e901c3368dfe6075532aba253f5c410', '5c14d0a4f4695c90b96cacf599c86c36dfbdd0060', 'c4b0eedb325a33aad9c23de71eecbbb051507a8b0', 'fe1558f981d067684aed9c77119629787b1b5fe40', '963419210fb08f2aa62c99163c70b19ddc5d12450', '146f5e4c1025ea9e38e3cd363db3240a850eb7080', '9a9ed6ab82a62c1541d01a5db136482819c444150', 'b058bd07390c81ea4567f2f390e587ea211a391a0', 'b59ba6075d94e2b864f7cd78d0fc7d74014b231a0', 'fabe58d50e585ebf9996d43ebd97d816bb787d2c0', 'cfbc3246d76eca63c04b56cb9685c51d2c6e3e9d0', '6670413f52009c5f2a38012ea099c0efc3011dad0', '88cdaa2ce1def388e71456823f356844f4bf224d0', 'aa58909346e674b70535a6e15bbf26e3ce72dbc00', '717dc346628a16019226b46f76f30e63881f7a0b0', 'a209dfcf89d01dfc97bf6af5d51d3eab4df675610', '59d6bf6b3952016638251db551c8b84f32a878ac0', '9b67c2348b572bedbde448262bd0e30b40b515560', '3f8e6959f3b5d9fcac5b2117bf1afb1afc22cee10', 'f6a51d77a648e22d2e15fbb909e17cb9248d50530', 'c09281baf8967f4c9fec89b7d42866999f1aa8bd0', '14168a05d5e59b95bdbd09e37a742d4dcbbee17f0', '5f57767c8673967a458479c5d3fc78738ce244490', 'fed25ab0c1a2a3cc85a51445504133f3fd7bc74b0', '1034ba4698f6664d2f5b562d6ec3eb57d1f9f8520', '9f6422a2cc2f27a002070a025f22da5dbc9764900', '35f36bb64e573cfa6d1c0f2ecafea72a3b200d0a0', '2dbb9e79bac72175122402957b503793fde374a30', 'bceebd5b985c122eaa01d2a48cdb0898db89d7820', 'a3c7ab3ef31a763aa80784af324d9f707c47021d0', '17f2b31cf18f2344e01e8e45c5a5420f3280c2410', 'a1856ecc216e0978e2e0ef139529f72a6015f0700', '0cffa99702067aa5221ea09188499a8714f510ae0', '977536acc7d9cfa18a632c649958968588709e330', '421c16b9de4d4ab6f171f9e22a7f2c7f4afb62b50', '8a2a91730cfe8ff4c5353b2f64eb14b4ebfd438a0', 'f4780eaeac2edb5a7e1bb78b58fc7b48b73f04500']		

		
		counts = {}
		for year in years:
			counts[year] = []
		countsummer = self.file_add( '/afs/crc.nd.edu/user/p/pivie/census/count_sum' )

		'''
		# Count words ocurrences in the data
		counter = self.file_add( '/afs/crc.nd.edu/user/p/pivie/census/count' )
		for year in years:
			for k,nkey in enumerate(normalized_data_keys[year]):
				cmd = "./count.%i < input_data > output" % (year)
				ckey, = self.call_add( returns=['output'],
					env=self.nil, cmd=cmd,
					args=[counter, nkey], params=['count.%i'%year,'input_data'] )
				counts[year].append( ckey )
		
		
		# Summarize words ocurrence counts by year
		for year in years:
			ar = counts[year]
			ar2 = ['input'+str(i) for i in range(1,len(ar)+1)]
			cmd = "./count_sum.%i %s > output" % (year, ' '.join(ar2))
			skey, = self.call_add( returns=['output'],
				env=self.nil, cmd=cmd,
				args=[countsummer]+ar, params=['count_sum.%i'%year]+ar2 )
			counts[year] = skey

		for year in years:
			print 'counts[%i] = \'%s\'' % (year, counts[year])
		'''
		'''
		counts[1850] = 'eec7ed8f8813475035b0f048a4bae7e5a443129e0'
		counts[1860] = '658c1bc7a572524c1cffab3f4eda575c98b3a43c0'
		counts[1870] = 'a4bdfff4b9fc2e2648323bf8e3e474d2e0b1679d0'
		counts[1880] = '35805dbf1f178f90200f9c26f7dd469e3b830a810'
		counts[1890] = 'c460b8d79fb60bd1d3262591290d5b54f2daf3f80'
		counts[1900] = 'ff9f93785fb5be6767372c207aecfba96f19e3730'
		counts[1910] = '753bd12b854bfc26bacd5f9372352f758aed669d0'
		counts[1920] = '011e605053be8a29410a6aa5c082edc3974256300'
		counts[1930] = '80446c16fd204874d8eb9e30d1c42154a09370da0'
		counts[1940] = '4cca08cf151f155b7ba0fefaf24e1e1b477101e10'

		frequencies = {}
		# Sort most frequent words (by year) to the top
		for year in years:
			frequencies[year], = self.call_add( returns=['output'],
								env=self.nil, cmd="sort -t\: -rnk3 input > output",
								args=[counts[year]], params=['input'] )
			self.file_dump( frequencies[year], 'frequency_%i.txt'%year )

		

		# Summarize total words ocurrence counts
		ar = counts.values()
		ar2 = ['input'+str(i) for i in range(1,len(ar)+1)]
		cmd = "./count_sum.all %s > output" % (' '.join(ar2))
		counts_all, = self.call_add( returns=['output'],
							env=self.nil, cmd=cmd,
							args=[countsummer]+ar, params=['count_sum.all']+ar2 )


		# Sort most frequent words to the top
		frequent_words, = self.call_add( returns=['output'],
								env=self.nil, cmd="sort -t\: -rnk3 input > output",
								args=[counts_all], params=['input'] )
		self.file_dump( frequent_words, 'most_frequent_words.txt' )
		print 'frequent_words = \'%s\'' % (counts[year])
		'''

		frequent_words = '4cca08cf151f155b7ba0fefaf24e1e1b477101e10'


		umbrella_env = self.env_add( engine='umbrella', spec='redhat6.5.umbrella' )

		fields = ['CY','CS','CC','CT','HS','FM','PN', 'FN','GN','BY','BP','SX', 'RL','ET','RC','AG','ID']
		filtered = {}
		'''		
		for field in fields:
			# Sort most frequent words to the top
			
			filtered[field], = self.call_add( returns=['output_data'],
									env=umbrella_env, cmd="grep '^%s:' /tmp/input > output_data" % (field),
									args=[frequent_words], params=['input'] )
			#self.file_dump( filtered[field], 'most_frequent_%s.txt' % (field) )

		for field in fields:
			print 'filtered[\'%s\'] = \'%s\'' % (field, filtered[field])
		'''
		filtered['CY'] = '6fbe0e12a525e77eb14a5b84f3948568448feb960'
		filtered['CS'] = '8c696891d98601ef7d4b4c1b0d253544f90146270'
		filtered['CC'] = 'd61deb431447e8215b0ef47d74656129a428a5d40'
		filtered['CT'] = '8cd56e6b6b6c0671e130e6774a9533c02e63513c0'
		filtered['HS'] = '2b77ae87429c86d11ceb491bdc38ea76561afb7c0'
		filtered['FM'] = '6b51de9921a6580a4e0bf7abdc1f746508c904b80'
		filtered['PN'] = 'd459bde19a83e46e0571b629c522860e6f7b1cdd0'
		filtered['FN'] = '7f0940cd24e6f9810259ae63419402afd8ee5fe60'
		filtered['GN'] = 'dd3daf26c50ca8d5cd7954a6e0ba3ba0aa2f70c60'
		filtered['BY'] = 'd110d79bff78ef5d973ffdf97402146dab5c29b30'
		filtered['BP'] = 'd3719cbed47d660ffeab4ef8be8b11fc501040870'
		filtered['SX'] = '93a54d86b05ace71794101a7dbb1437220dcd5bb0'
		filtered['RL'] = 'f12173e8a7e0d301055c97af729499386472957c0'
		filtered['ET'] = 'ea477d187d5ba8ed57acc2a60557634d5c363fab0'
		filtered['RC'] = 'ea3c11129e8f84796038e370e52ae6c728181e1e0'
		filtered['AG'] = '991952d8fc2582dedf5ae99b97119d5e15d814260'
		filtered['ID'] = 'e7b929524af8775c6c50066f8654d24467bdd86c0'


		#redhat6_5_jellyfish_env = self.env_add( engine='umbrella', spec='redhat6.5.umbrella', targz_env=jelly_fish_env )
		#print redhat6_5_jellyfish_env
		
		try:
			compare_words = self.file_add( '/afs/crc.nd.edu/user/p/pivie/census/compare_word' )
			#print compare_words
			for i in range(2000,2500):
				top_matches, = self.call_add( returns=['output_data'],
										env=jelly_fish_env, cmd="python compare_word input_data %i > output_data" % (i),
										args=[filtered['FN'],compare_words], params=['input_data','compare_word'] )
				#top_matches, = self.call_add( returns=['output_data'],
				#						env=redhat6_5_jellyfish_env, cmd="python /tmp/compare_word /tmp/input_data %i > output_data" % (i),
				#						args=[filtered['FN'],compare_words], params=['input_data','compare_word'] )
				self.file_dump( top_matches, 'tops/%ssimilarities_%i.txt' % ('FN',i) )
		except KeyboardInterrupt:
			print traceback.format_exc()
		
		
	def demo_merge_sort( self ):

		E1 = self.env_add()
		D1 = self.file_add( 'nouns.txt' )
		D2 = self.file_add( 'verbs.txt' )

		D3, = self.call_add( returns=['output.txt'],
			env=E1,	cmd='sort input.txt > output.txt',
			args=[D1], params=['input.txt'] )
			
		
		D4, = self.call_add( returns=['output.txt'],
			env=E1, cmd='sort input.txt > output.txt',
			args=[D2], params=['input.txt'] )
		
		
		E2 = self.env_add(engine='targz', folders2include='libraries')

		D5,D6 = self.call_add(
			returns=['merged_output.txt','file_not_here.txt'],
			env=E2, cmd='sort -m input*.txt > merged_output.txt',
			args=[D3,D4], params=['input1.txt','input2.txt'] )

		
		self.file_dump( D5, 'merged_result.txt' )
		self.plan_dump( D5, 'merge_sort.dump', depth=3 )



	def env_add( self, **kwargs ):
		if 'engine' in kwargs:
			if kwargs['engine'] == 'targz':
				filename = 'tmp_' + str(uuidlib.uuid4()) + '.tar.gz'
				zip_cmd = "tar -hzcvf %s %s" % (filename, ' '.join(kwargs['folders2include']))
				p = subprocess.Popen( zip_cmd , stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell=True)
				(stdout, stderr) = p.communicate()
				#print 'stdout:',stdout
				#print 'stderr:',stderr
				
				key, length = self.hashfile( filename )
				mesg = Mesg( action='have', key=key )
				self.out_msgs.put_nowait( {'mesg':mesg} )

				fl = File( key=key, path=filename, size=length )
				self.files[key] = fl

				obj = {'engine':'targz', 'args':[key], 'params':['_folders2include.tar.gz']}

				keyinfo = hashlib.sha1()
				keyinfo.update( str(obj) )
				env_key = keyinfo.hexdigest()
				
				mesg = Mesg( action='save' )
				en = Envi( key=env_key, body=obj )
				# Send the server this environment specification
				self.out_msgs.put_nowait( {'mesg':mesg, 'body':str(en)} )

				return env_key
					
			elif kwargs['engine'] == 'umbrella':
				
				filename = kwargs['spec']
				key, length = self.hashfile( filename )
				mesg = Mesg( action='have', key=key )
				self.out_msgs.put_nowait( {'mesg':mesg} )

				fl = File( key=key, path=filename, size=length )
				self.files[key] = fl

				obj = {'engine':'umbrella', 'args':[key], 'params':['spec.umbrella']}
				if 'targz_env' in kwargs:
					obj['targz_env'] = kwargs['targz_env']

				keyinfo = hashlib.sha1()
				keyinfo.update( str(obj) )
				env_key = keyinfo.hexdigest()
				
				mesg = Mesg( action='save' )
				en = Envi( key=env_key, body=obj )
				# Send the server this environment specification
				self.out_msgs.put_nowait( {'mesg':mesg, 'body':str(en)} )

				return env_key
		else:
			return self.nil

	def file_add( self, filename ):
		key, length = self.hashfile( filename )
		mesg = Mesg( action='have', key=key )
		self.out_msgs.put_nowait( {'mesg':mesg} )
		
		transfer_now = False
		if transfer_now:
			data = ''
			f = open( filename, 'rb' )
			buf = f.read(1024)
			while buf:
				data += buf
				buf = f.read(1024)

			fl = File( key=key, body=data )
			self.files[key] = fl  # save the data in case the server asks for it

		else:
			fl = File( key=key, path=filename, size=length )
			self.files[key] = fl

		return key

	def data_add( self, data ):
		#data = jsonlib.dumps(data, sort_keys=True, indent=4, separators=(',', ': '))
		key = self.hashstring( data )
		mesg = Mesg( action='save' )
		fl = File( key=key, body=data )
		# Send the server this file
		self.out_msgs.put_nowait( {'mesg':mesg, 'body':str(fl)} )
		return key

	def file_dump( self, key, filename ):
		mesg = Mesg( action='send', key=key )
		# Ask the server to send this data
		self.out_msgs.put_nowait( {'mesg':mesg} )
		# save the filename for when the data comes
		self.dump_files[key] = filename
		return key

	def call_add( self, returns, env, cmd, args=[], params=[], types=[], env_vars={}, precise=True):
		obj = {'returns':returns, 'env':env, 'cmd':cmd, 'args':args, 'params':params, 'types':types, 'env_vars':env_vars, 'precise':precise}
		if len(types)==0:
			for param in params:
				obj['types'].append('path')   # alternative to 'path' is 'data'

		keyinfo = hashlib.sha1()
		keyinfo.update( str(obj) )
		key = keyinfo.hexdigest()
		
		mesg = Mesg( action='save' )
		cl = Call( key=key, body=obj )
		# Send the server this call specification
		self.out_msgs.put_nowait( {'mesg':mesg, 'body':str(cl)} )
		
		results = []
		for i in range( 0, len(returns) ):
			results.append(key+str(i))
		return results
		#env, paper, dist, case
		

	def plan_dump( self, key, filename, depth=1 ):
		plan = {'key':key,'depth':depth}
		mesg = Mesg( action='send', key=key, plan=plan )
		self.out_msgs.put_nowait( {'mesg':mesg} )
		self.dump_plans[key] = open( filename, 'wb' )
		return key











		
	def demo_hep( self ):

		D0 = self.file_add( 'cms.umbrella' )
		E1 = self.env_add( engine='umbrella', args=[D0], params=['umbrella_spec.json'] )
		D1 = self.data_add( '20' )
		D2 = self.data_add( 'CMSSW_5_3_11' )
		D3 = self.data_add( 'slc5_amd64_gcc462' )

		cmd = '''
. /cvmfs/cms.cern.ch/cmsset_default.sh

scramv1 project -f CMSSW ${CMS_VERSION}
cd ${CMS_VERSION}
eval `scram runtime -sh`
cd ..

cmsDriver.py SinglePi0E10_cfi --conditions auto:startup \
-s GEN,SIM --datatier GEN-SIM -n ${NumEvents} \
--relval 25000,100 --eventcontent RAWSIM
		'''

		D4, = self.call_add(
			returns=['SinglePi0E10_cfi_GEN_SIM.root'],
			env=E1, cmd=cmd,
			env_vars={ 'NumEvents':D1,
				'CMS_VERSION':D2,
				'SCRAM_ARCH':D3 } )




	def demo_hep2( self ):		




		cmd = '''
. /cvmfs/cms.cern.ch/cmsset_default.sh;

scramv1 project CMSSW ${CMS_VERSION}
cd ${CMS_VERSION}
eval `scram runtime -sh`
cd ..

cmsDriver.py SinglePi0E10_cfi_GEN  --datatier GEN-SIM-DIGI-RAW-HLTDEBUG --conditions auto:startup -s DIGI,L1,DIGI2RAW,HLT:@relval,RAW2DIGI,L1Reco --eventcontent FEVTDEBUGHLT -n ${NumEvents}
		'''

		D5, = self.call_add( 
			returns=['SinglePi0E10_cfi_GEN_DIGI_L1_DIGI2RAW_HLT_RAW2DIGI_L1Reco.root'],
			env=E1, cmd=cmd,
			args=[D4],
			params=['SinglePi0E10_cfi_GEN_SIM.root'],
			#types=['filename'],
			env_vars={ 'NumEvents':D1, 
				'CMS_VERSION':D2, 
				'SCRAM_ARCH':D3 } )


		D5, = self.call_add( 
			returns=['SinglePi0E10_cfi_GEN_DIGI_L1_DIGI2RAW_HLT_RAW2DIGI_L1Reco.root'],
			env=E1, cmd=cmd,
			args=[D1,D4],
			params=['$1','SinglePi0E10_cfi_GEN_SIM.root'],
			types=['string', 'filename'],
			env_vars={ 
				'CMS_VERSION':D2, 
				'SCRAM_ARCH':D3 } )


		D5, = self.call_add( 
			returns=['SinglePi0E10_cfi_GEN_DIGI_L1_DIGI2RAW_HLT_RAW2DIGI_L1Reco.root'],
			env=E1, cmd=cmd,
			args=[D1,D4],
			params=['NumEvents','SinglePi0E10_cfi_GEN_SIM.root'],
			types=['env_var', 'filename'],
			env_vars={ 
				'CMS_VERSION':D2, 
				'SCRAM_ARCH':D3 } )



		cmd = '''
. /cvmfs/cms.cern.ch/cmsset_default.sh;

scramv1 project CMSSW ${CMS_VERSION};
cd ${CMS_VERSION};
eval `scram runtime -sh`;
cd ..;

cmsDriver.py SinglePi0E10_cfi_GEN --datatier GEN-SIM-RECO,DQM --conditions auto:startup -s RAW2DIGI,L1Reco,RECO,VALIDATION,DQM --eventcontent RECOSIM,DQM -n ${NumEvents}
		'''


		D6,D7,D8 = self.call_add( 
			returns=['SinglePi0E10_cfi_GEN_RAW2DIGI_L1Reco_RECO_VALIDATION_DQM.py', #D6
				'SinglePi0E10_cfi_GEN_RAW2DIGI_L1Reco_RECO_VALIDATION_DQM.root', #D7
				'SinglePi0E10_cfi_GEN_RAW2DIGI_L1Reco_RECO_VALIDATION_DQM_inDQM.root'], #D8
			env=E1, cmd=cmd,
			args=[D5],
			params=['SinglePi0E10_cfi_GEN_DIGI2RAW.root'],
			env_vars={ 'NumEvents':D1, 
				'CMS_VERSION':D2, 
				'SCRAM_ARCH':D3 } )



	
		self.file_dump( D7, 'SinglePi0E10_cfi_GEN_RAW2DIGI_L1Reco_RECO_VALIDATION_DQM.root' )
		





	def get_pathname( self, hash_index ):
		return './' + hash_index

	def debug_print( self, *args ):
		if self.debug:
			for arg in args:
				print arg,
			print ''



	def receive( self, sock, objects ):
		
		if len(objects) <= 0:
			return
		mesg = body = None
		if len(objects) > 0:
			mesg = Mesg( objects[0] )
		self.debug_print( '---->', self.role, ':   \n', mesg)

		if mesg.action == 'send':
			fl = self.files[mesg.key]
			mesg.action = 'save'
			self.out_msgs.put_nowait( {'mesg':mesg, 'body':fl} )
			#self.send( sock, {'mesg':mesg, 'body':fl} )


		elif mesg.action == 'have':
			if mesg.key in self.files:
				del self.files[mesg.key]


		elif mesg.action == 'save':
			objs = []
			i = 1
			while i<len(objects):
				obj = objects[i]
				if (i+1)<len(objects):
					body = objects[i+1]
				else:
					body = None

				if obj['type'] == 'envi':
					en = Envi( obj, body=body )
					objs.append( en )
					self.debug_print( en )
				elif obj['type'] == 'call':
					cl = Call( obj, body=body )
					objs.append( cl )
					self.debug_print( cl )
				elif obj['type'] == 'file':
					fl = File( obj, body=body )
					objs.append( fl )
					self.debug_print( fl )
				elif obj['exec'] == 'exec':
					ex = Exec( obj, body=body )
					objs.append( ex )
					self.debug_print( ex )
				i += 2

			if mesg.plan:
				pl = self.dump_plans[mesg.plan['key']]
				for obj in objs:
					pl.write( str(obj) + "\n" )
				pl.close()
				del self.dump_plans[mesg.plan['key']]

			else:
				filename = self.dump_files[mesg.key]
				obj = objs[0]
				'''
				if obj.size>200:
					print str(obj)[0:200]+'...'
				else:
					print obj
				print ''
				'''
				print 'Saved file %s as %s.' % (obj.key, filename)
				if obj.path:
					shutil.move( obj.path, filename )
				else:
					f = open( filename, 'wb' )
					f.write( str(obj.body) )
					f.close()
				del self.dump_files[mesg.key]

		else:
			print 'unknown action: %s' % meta

		return True

		

	def send( self, sock, msg ):
		self.debug_print( '<----', self.role, ':   \n', msg['mesg'] )
		sock.sendall( msg['mesg'] + "\n" )
		
		if 'body' in msg:
			self.debug_print( msg['body'] )
			sock.sendall( msg['body'] + "\n" )

		sock.sendall( '\n' )




	def __init__( self, hostname, port):
		self.hostname = hostname
		self.port = port
		self.start()

	def start( self ):

		self.out_msgs = Queue()
		self.in_msgs = Queue()
		self.files = {}
		self.dump_files = {}
		self.dump_plans = {}
		self.request_cnt = 0


		try:
			self.sock = socket( AF_INET, SOCK_STREAM )
			try:
				self.sock.connect(( self.hostname, self.port ))
				fcntl.fcntl(self.sock, fcntl.F_SETFL, os.O_NONBLOCK)
				self.ready = True

				
				self.connection_thread = Thread( target=self.handler, args=([ self.sock, self.in_msgs, self.out_msgs ]) )
				self.connection_thread.daemon = True
				self.connection_thread.start()


			except KeyboardInterrupt:
				self.sock.close()
			except error as msg:
				self.ready = False
				self.sock.close()
		except error as msg:
			self.sock = None


		self.demo_census()

		try:
			while True:
				if len(self.files)<=0 and len(self.dump_plans)<=0 and len(self.dump_files)<=0:
					# Exit when complete
					break
				time.sleep(1)
		except KeyboardInterrupt:
			raise KeyboardInterrupt	

		self.stop()

	def stop( self ):
		self.sock.close()

	def restart( self ):
		self.stop()
		self.start()


	#def set_ui( self, ui ):
	#	self.ui = ui



	def handler( self, sock, in_msgs, out_msgs ):
		mybuffer = ''
		retries = []
		#sock.settimeout(1)
		parser = Parser(file_threshold=1024)
		while True:
			now = time.time()
			
			while not out_msgs.empty():
				msg = out_msgs.get()
				self.send( sock, msg )

			
			for i,ar in enumerate( retries ):
				meta, ready_time = ar
				if ready_time < now:
					del retries[i]
					if not self.receive( sock, meta, retry=True ):
						retries.append([ meta, time.time()+3 ])
			
			try:
				raw_data = sock.recv( 4096 )
				if len(raw_data) <= 0:
					# If timed out, exception would have been thrown
					print '\nLost connection...'
					sys.exit(1)
					break
				

				results = parser.parse( raw_data )
				if results:
					for objects in results:
						self.receive( sock, objects )


			except error, e:
				err = e.args[0]
				if err == errno.EAGAIN or err == errno.EWOULDBLOCK:
					time.sleep(1)
					continue
				else:
					# a "real" error occurred
					print e
					sys.exit(1)

				raw_data = ''

		self.stop()


	def hashfile( self, fname, blocksize=65536 ):
		key = hashlib.sha1()
		afile = open( fname, 'rb' )
		buf = afile.read( blocksize )
		length = len( buf )
		while len( buf ) > 0:
			key.update( buf )
			buf = afile.read( blocksize )
			length += len( buf )
		return key.hexdigest(), length

	def hashstring( self, str ):
		key = hashlib.sha1()
		key.update( str )
		return key.hexdigest()
