
import sys
sys.path.append('../')
import pandas as pd
from lib.path import data_file_path
from lib.clean import float_to_int_str


def filter_out_non_duplicates(df):
    #  creates a df in which a given tracking_number must appear more than once
    df[['uid', 'tracking_number']].groupby("tracking_number").filter(lambda x: len(x) > 1)

    #  if a given uid is associated with a given tracking number more than once, drop it
    df['uids'] = list(zip(df.uid, df.tracking_number))
    df = df[['uids']]
    df = df.drop_duplicates(subset=['uids'])

    #  creates a df where a given tracking_number is associated with atleast two persons
    df['uid'], df['tracking_number'] = df.uids.str

    df.loc[:, 'tracking_number'] = df.tracking_number.fillna('')
    return df[~((df.tracking_number == ''))]


def groupby_tracking_number_and_uid(df):
    df = df.groupby('tracking_number')['uid']
    df = pd.DataFrame(df).astype(str)
    df.columns = ['tracking_number', 'uid']
    df = df.replace(r'\n', ', ', regex=True)

    df.loc[:, 'uid'] = df.uid.str.lower().str.strip()\
        .str.replace(r'\, (\w+) ', '', regex=True)\
        .str.replace(r'^(\w+)  +', '', regex=True)\
        .str.replace(r'\,? name(.+)', '', regex=True)\
        .str.replace(r'^(\w+)$', '', regex=True)\
        .str.replace(r'  +', ', ', regex=True)
    return df[~((df.uid == ''))]


def generate_duplicate_df(df):
    #  this function creates a df containing only rows that are duplicative
    #  i.e., a df in which a tuple of uids are associated with more than one tracking number

    # ran this code in the terminal to generate the list below
    df = df[df.duplicated(subset=['uid'], keep=False)]
    return df


def search_for_imperfect_duplicates_in_main_df(df):
    uids = df.loc[:, 'uid']
    list_of_str = [
        '2d2cad8b9201f9d047ed71b5f54265f1, 7fc8478fc39de75b7a62d1d34aaed723',
        'b27a1368deb29f3149331174ca039fe7, 938d3c343808f611af8a2e9e5e003eab',
        'fd518aae7d239c6271c1bdea3d4fe667, 9cdea51892acc13176674ab5d46f1c12',
        '3b05bfc7104476d5ad92919603ab20fe, e9a4f6357b58faab78d5c0dcf45429ac',
        'd71bf1cd90cdf47820f7be40b7e0df7a, a37118ae7d47adebb3e229a4e3eb7a8c',
        '688385092978c43c80f41144e87f2903, 849579bdcbe5c177539e87acc67370f5',
        '05855898787ffe1d549c557cc6af9c35, 811fc7d403ed28862968d8828da14caf',
        '07a4560928b4f3165895eb614462bff0, 489867d93655f86e158a435fcd856b7e',
        '8c8019d97c629ecec9eeaeb32bebcdaa, 95f08412474699273d4b356a53b68157',
        '9fd5e68d2a6ac543bc2b27df3cce27a9, d56a0c9d9248ca71c68a76a024d098ba',
        'e79641787ce3b91bf87d69e1dcfa476b, 461cbc934bd2162cb673433efa9f9e98',
        '5eb8afe2d8dd53d50b283bd46ed9fe88, 3168df0fb09a827b5e5af213bd8d77f3',
        '7f16a46202b6b5fb0410cb520a6626bf, 8d67e7d4dbbdcce80a456bbb0b1b4f30',
        'fc51ea6bf38b0110ddced4bad6f7e123, 434d550ca20dceef47ae997e760589e6',
        'dd7d6d78192257af0a7e4b1a8c233a87, 812aaa5877a0d2109641824217b123f4',
        '862e2b0840d4d6fe559688969e08a73f, 3e943f0070da95fee6f290df4195e9bf',
        '5a03a97b64227c97847d872d34cc1a73, b21dc1accbd00e85310e825cd0e5dc36',
        '5537fcbe001f5d90fbe905d301cbf56f, 88f20255aa0dd63e836f50a563094a98',
        'bfc5b962e33c229da6c76ce4355127f3, 6668fef0d72156616dc246a4bfa8d31d',
        'd834c20e5497a6c317c9090c7fd9ba61, e687f2cc52299d0f229c27fd8281c2cb',
        'd12b80b48eb3d5159cadb1e47b9161d4, 829d7bd54fcf4453faa899b95f369e3c',
        '8581843a2a5769e3d5bdfa4e1cd8f7b7, fa51870375cc21e26fe79921d6ede2ec',
        '4cd1dd5e283d0f1619b6ecce87716686, f861ca8c318d1d9b54e938231a6ff066, b6a2ff826b181e8c002b29a52760d8a5',
        '31db5b16a5e45324bdc7c71efc4b1adb, 4a2808c23c3a3daaf4ea3ada0534dd8b',
        '998fb9b7c690e57f95d815fea53e42aa, 1cedf07e38aa8cfadde3e4d5ca2a60db',
        '42a91489aecf0685062798148ae65c1d, 39fbf7ae5c6a03fd8f21a1eb37327c6e',
        '21be5cb8257dd88e02878d204fda9a71, d357f219648414fd009c2f18556ccca7',
        '4cd1dd5e283d0f1619b6ecce87716686, b6a2ff826b181e8c002b29a52760d8a5',
        'bff90b3018c8e850c821b3f99d0d549b, b004228845abef079d60f64cd3fbe6a8',
        'abb1ba520815506443ec332a4d314b42, e95ac37e8a8a6605087ec859bd120b25',
        '9f15b694746a82077ea907d146460204, b718e13d324c7af5fc7d0c3d20bcdda8',
        'b0b14115bbcbaf8c9cda2b9f862caa3b, 3981d9d06cd892e3e4a08a883f3ea110',
        '11a28582878f216bc755143a2d916766, ddb1b2849b45246c131c59ea8719c573',
        '9a29377a445ba39f200d02659a778e84, 21a7431600b91a8bd3b90fb6967bd749',
        '7d8b1ffa9ece15d6a09c3b51a2df8263, 738dc5819c13a6a029139dfd4b962cea',
        '40a81cb1763e73c11b5dbd12373f8e75, ec312e50f9556c6c399467d790a74dba',
        'a92151ceea629aceabfbd47b4d81a377, 9bf29343b750fc7c84dceee3d4a821a9',
        '6dbe073a32eb236a1536059bb8370b01, 54333cd316d64bb88661c103f5339e99',
        '5176ddbaea27773b86c002f4f727d5e4, 698b04936ebe9759d0640c06a9ac08f8',
        '7fded356fa441b638e9d5366cf472349, 804ddcc0fdf726ecf37e8dbc13d2fca4',
        '48ef64366ca60c1dd97a0c5c4573383c, d16ef25d03cca05b010ade1f7aa8617f',
        '745d42c62f6f9febdb72c2b3fe26db4e, fe557c179e2509d78c136447da10a1d4',
        'fde643a0c48b129514469a4f03890fab, a426587718ed515badb4a8995a769074',
        '8f7bc13a1d2568e8a8703538111c8a9e, 929724da95e5321a723b2daa2b0925a1',
        'a92151ceea629aceabfbd47b4d81a377, 782176883cf8ed0454afa9780b6d26ed',
        '8bd656ab1208b742ca7e38c39115d3e9, 57d75b43a1c95320571bd40f496b9843',
        '5bf2e99f614308b1585ad90c68f52e7b, 5176ddbaea27773b86c002f4f727d5e4',
        'af7244139bd41feebb90ea16a0a94d59, 9fd5e68d2a6ac543bc2b27df3cce27a9',
        '862e2b0840d4d6fe559688969e08a73f, d56a0c9d9248ca71c68a76a024d098ba',
        '6ad7ddff54a5a75f25e88408a456360d, eeeecf330be15848fbb35612fb4308e0',
        'cf0dfc6aff145c5943a5e8306dc65f52, ad4c7223dfd526650dfe3dd685d9035d',
        'cfd727667b3e4024626c98485709ee8f, da951f3e7d49310e15a6ebe8183bd57b',
        '9e39d1183019c266060cdaba3cef36da, 2868360c5cb4ca7f7df19134f23f11e4',
        '536065fa330619cd5bb6c604dfd47a8e, cb4217522eb7823ab354ce1c226200fe',
        '824db5f124b89e6db216045f6eeff763, 6b342e1d31ad5b88cc865d08661c6cfb',
        '0644dbac0123fbed6f334629a180740a, 3ba732f2bfa3a3bcc258d2b2730beac4',
        '866dcd3a00cfd71d5d6333e22d1a6e80, 48c96df11575747c4e73424bbcf08b76',
        '19503ef3a3e5d0d04ebeb9d0b641fcaa, b532165813cd5991a18cbf0241f45bd6',
        '7e8a42e23405c920d2c250b864c499d4, 1cc183049bb463d599b8341eff11d9d1',
        '862e2b0840d4d6fe559688969e08a73f, cd1cb756eed1e7a96b05dde0416a79a6',
        '0e8d5195ad09b243d1bda3fcd3704d41, 1a2b332768b0c1320659688d257b80a1',
        '452194615f5715b93ad207fe2732362a, 9c9b2d9f6d4668c2b1bfa433c46239bd',
        '2868360c5cb4ca7f7df19134f23f11e4, 5a49e2fefe272939431d7c81f57ea03d',
        '872744fefc51dd1634e81ae10379ef6e, 1cc183049bb463d599b8341eff11d9d1',
        '095116a826eefbc098e7325613c0e10f, d71bf1cd90cdf47820f7be40b7e0df7a',
        '8dc672b4dddc928b6eea015d213225ec, a125aa287edac2b6137cb36de6aabc55',
        'c8283035ac03e0260c558cb2c05a3d81, ad568ab1f557e35482ca8341e08a4916',
        '8e76e6643edc61dd8a6958a25a347c49, 40c2bcc1073aa8e793f0578ac2eb8ff7',
        'fe1fac70b2c78e3e496f9b2b6620fb1d, 55f9bdafbc43aea245eec93b273de211',
        '9a804569e868e7ab7a30c932897cd450, fe557c179e2509d78c136447da10a1d4',
        'cf6d7868076f9556217929eb501df1f3, 40e190ec5d4a63c4e397e77f1410d2b5',
        '7fded356fa441b638e9d5366cf472349, 8a0e9fe0c2304a51490d5fed79cff8e8',
        '0fef10741b1e434a59e8a01008c454b6, 1716776a1a5e6f49ead3c49960d040ea, 21a7431600b91a8bd3b90fb6967bd749, 7fded356fa441b638e9d5366cf472349, 8a0e9fe0c2304a51490d5fed79cff8e8, 804ddcc0fdf726ecf37e8dbc13d2fca4',
        '06d90cac7f6f34f4e560590a1cd28ff5, 34e41b57776a583b1354f811e418ab13',
        'a8f58149008252ea6670a55cc1e110c9, a7bd927fa494bf6e892b481c7ff3bc90',
        '57d75b43a1c95320571bd40f496b9843, b167b1731956ed9e517d01c0351e62e2',
        'cb8a5b287b058005bea639cd0ee51e2d, 5cad6cdcd18c5ab130bfb803578e1ce7',
        '0fef10741b1e434a59e8a01008c454b6, 1716776a1a5e6f49ead3c49960d040ea',
        'c0939da7612acdd877b80208f2ee88e2, e421b7a701901373e77c183b7c59f92c',
        '4c1cde0bd982da2e36698be2add02dc4, e0738b9f45a8fc87534b09de53cb2c19',
        '597e05e0e8ba8d8f77fdf02a93a66cc6, 767e9640b3958a949b38f85eb7381935',
        '74044e2aa0e759e6e59d1745a9d77030, a92f65aa5ad624fa2029670e6dbb4d14',
        'b3cea4d17380e374f4f1b3173cf3286a, 88f20255aa0dd63e836f50a563094a98',
        'd9c00d2b9cd4a7c1b7fd29f23e093ced, 9b98cc6f10056b7cb39f4bb8de39f370',
        '5a1c0149be16cb6dda9cdf5fbebedced, 574160f26caeaba79a00c6a55fe7025d',
        '06087d0c427fabb45b1128f211dc8b02, bce208e384f94bcfba08f4a9201c7eee',
        'b004228845abef079d60f64cd3fbe6a8, 6b3f558f5ef8a4c2ba15fcab9e3c75df',
        '7ff5afaeefc8b92c424a3911e4c82c46, 500cd5c133f4bbcec9b02ee3984d51ff',
        'da5d8837e26f74c81770d82f1164eef1, ddcf5e1e1b386bfc2306c6aa7ff09cff',
        '2c7336ce9028f4075b2d9e1bd505db33, 47968016e41e467c0ab8d12f677f50fa',
        'da951f3e7d49310e15a6ebe8183bd57b, 04e1eabc6855cd695170098d62200cdc',
        '472a288bdff8ea04216438cf29b99ee1, 5cad6cdcd18c5ab130bfb803578e1ce7',
        'c0b2979ffca6b25ab2c50c8af4f6a96e, e9b97c9ef0752e3b1abe967dc3d7bf56',
        'b0886e7abc8523158397ee96b06a23a9, 5997b252fe9d770d72bdf825ac772fb5',
        'e7b9be6329fe62a598e523dca2540eef, 1a2b332768b0c1320659688d257b80a1',
        '7e07e21854c3766cbec26df225817c73, 1e7493dfd8ef5cc45a43c36b988f8e6b',
        'a92151ceea629aceabfbd47b4d81a377, d5fcc00966878c2235b5e7ae38d75834',
        'da951f3e7d49310e15a6ebe8183bd57b, 3602ce30305db2ce8982ec7d09f50580',
        'b9bdaead78afc2613eae967071248482, 755f9ab5c3f55ca1a22cadf3d2ee7ec3',
        '27405d253b186046398ac2c0cc78111f, 1a610dbe0810ea7c84a99a60955b2012',
        '4aef072c378378f63549e64d3c8ddbe0, 18637135366c045943039d9584bafed3',
        '009cbd42a2df64f7605b4d3c44804f1f, 81ce213189ee816017229582c6d337ad',
        '4e973c5307167419176ae304f303e5d2, 2ffcc32216e566e4b3e2a475687bcbcd',
        '1c4c0da2ec11f8bc22f1de265f3ea450, 5c602533233cc852f8e0a3e96745e0eb',
        '4a8dbb9f8a0b1ffd35f54173d1227805, d16ef25d03cca05b010ade1f7aa8617f',
        '101b94ebbd24a7850b5234084db0e9b9, dfd5b1e5323cb94999e850e9a03ce536',
        'cfbbf8f7078d44c21c8828a6630a4ea2, 1c4c0da2ec11f8bc22f1de265f3ea450',
        'efa70126b59b8d6e346ee6843a84ae18, 71a2b79aabf3365b2f9733552fc50ea8',
        'cfbbf8f7078d44c21c8828a6630a4ea2, 7e8a42e23405c920d2c250b864c499d4, 1c4c0da2ec11f8bc22f1de265f3ea450, 48c96df11575747c4e73424bbcf08b76, 5c602533233cc852f8e0a3e96745e0eb',
        '612ed7a6de9c9b6627f92bf69cb58e6e, dd2611fdc897f6ea6d1d1712a99efaf0',
        '2a51b0f4ba0441fc9d4990bbf43a0229, be7a530710eacf5c09c3b3661fe2480e',
        '43922309682818253d4382961b058311, 20ba55d299c17a3c597c75694b79b726',
        '9bf29343b750fc7c84dceee3d4a821a9, a1c7708bffa4a4c013f9bb09c779c4bf',
        '727d84941df03b4608c0e4ce3a92a789, a8766923063b332e60028b7de346cbe0',
        'cbaaca1a04cc637d7569562cfb963c46, ce3f17eaadcd8fe61d817657f07478a4',
        'a595850cfb7b8fa04d46fbe85ac01a58, e652ed96f785dfbf21033309f213807f',
        'e7b9be6329fe62a598e523dca2540eef, 82117b6c6bdb99f8c43576f75aada8ed',
        '089d9dbee1015f3b78f18f51ba8c0327, 3ad21e5f29c38ae7b6a71523e15e25d1',
        '948e18131c00463dd0834a56ad8f89f8, a28b90107f235b7ca4de24cfdea934f8',
        'a7bd927fa494bf6e892b481c7ff3bc90, 7d8b1ffa9ece15d6a09c3b51a2df8263',
        'cfbbf8f7078d44c21c8828a6630a4ea2, 5c602533233cc852f8e0a3e96745e0eb',
        'f6e3f573d4e0e1b43e6bdcd785f20729, 56dc54ed1ed63238da84f7fdb03c7f6b',
        'd0889a354e5f09be54de12b65aa419f3, 8bd63c99dd5db3dc03be2964c9adf9d1',
        '88f20255aa0dd63e836f50a563094a98, 84da541d12c98a5810b04a29b20c84f1',
        'b9ad75756632e814700457e79f68fad3, e5bcb7256d9a3287980c5d7fe3845c12',
        '87e6bc59e482049bca6ab383422d5ae0, 906aee1703ea31fe5dccbdf1a53fadc3',
        'fe8b1dbc2a201a522ebc495a0333d162, d16ef25d03cca05b010ade1f7aa8617f',
        'f44f97853f78622b41e9977e1abe7866, d4251fce518146b9bf18c4727410ba33',
        '2a104ca3db8b221be3d4480231172abc, 650b2aa1886693483e1e589bc5eff898',
        'da5d8837e26f74c81770d82f1164eef1, 81497e98ef1bd38a24443516b2c5269c',
        '73e9d28ebf29a061f2c6f7353c018b0d, 2b91e61d15917cb21af19a03523d15d5',
        'd12b80b48eb3d5159cadb1e47b9161d4, 8201d54df2a40b0d1e8fac78e3483426',
        '6e28eae5c214351b589232d6928ba551, 306c539950e8c6aaaa2b09c0c269f461',
        'cbaaca1a04cc637d7569562cfb963c46, e990a37a007c2497fdb452408727ecd4, ddcf5e1e1b386bfc2306c6aa7ff09cff',
        'bff90b3018c8e850c821b3f99d0d549b, a1c4c12aba057b4b06344e7bcb2718ff',
        '1716776a1a5e6f49ead3c49960d040ea, a397984cfbe60ca52d142fe8a8963765',
        '92f12c3529615b297a2472b77961f625, d23be2ef4b9479ab490763147169b9ea',
        'f28f3c111a3d0832f0ec96a768912efe, 21be5cb8257dd88e02878d204fda9a71',
        'e990a37a007c2497fdb452408727ecd4, 3ba732f2bfa3a3bcc258d2b2730beac4',
        '4a8dbb9f8a0b1ffd35f54173d1227805, 81497e98ef1bd38a24443516b2c5269c',
        '223494ad0ae821a6beb4fece42ee28f2, 9115b10c2dd837f90e53110a7ac625d9',
        '1716776a1a5e6f49ead3c49960d040ea, 5997b252fe9d770d72bdf825ac772fb5',
        '74044e2aa0e759e6e59d1745a9d77030, 3ba732f2bfa3a3bcc258d2b2730beac4',
        '6a06e1a470912593c51418d2def7e858, 92f12c3529615b297a2472b77961f625',
        '281c01a1b8a07a56609c5b10a9e1c148, e5c6e1680c5e0d758cf5c6354f15a1c9',
        'b0f7be3fc290d4062c05e884a085148d, 3076f52bf765c928f9105e985c6207a9',
        'cbaaca1a04cc637d7569562cfb963c46, 00f192f666f2e6ebeb0ec04e948febbd',
        'ccad9784b3188744dc3fbd71590b7583, 21a71be2b52dc3f610acd7cc348c853a',
        '4e973c5307167419176ae304f303e5d2, 0651ff554b810356fe536026da09aa8e, da5ec1607a4cc8a44b4b7963426fba40',
        'a8766923063b332e60028b7de346cbe0, 500cd5c133f4bbcec9b02ee3984d51ff, ff669cc4b5878bd9d115fd1b6ac6c380',
        'a34b9fecb7c8e863b92e8aea8559cf51, 8e4d87ec7c60b8e685e15dd9ecea104c',
        '615431bff4472554462a8b6445a18585, 7c45171deb67212cdc3749a8046006ef',
        '129139c7ce91abf9b4e8783adcec04c6, c4960591ab54569bf512525d239c38b4',
        '2fee4778b97743ff6af445802d35c030, d00965a97518f81a20c9be784b96553e',
        '681c8fe326cc2ad5cb04506f52574132, 738dc5819c13a6a029139dfd4b962cea',
        'c75e6823cc3de53f0c4129af057acb4a, 40e190ec5d4a63c4e397e77f1410d2b5',
        'd601272d7877017e5959c8dc67750ffa, d21f42a3196ab533161a1c446fe6f4d5',
        '2b34d88c75e211c79055e5df7de66ecb, 71d3366908bf2f3d28d9e47cbbbe97a7',
        'a50b15e88522ca16ca4901b5363730cf, d71bf1cd90cdf47820f7be40b7e0df7a',
        '95f0da851956b555a435f511e6c76350, 19682fdd177ff6872cbef6f96a91c423',
        '38655452c1a964184caf910e5be0c8ee, cb2c3497b94007fb1429c501da3c77b2',
        '9aea0cd1efec57fa5b03d20f4d55b4be, 1a15535764f3158a07a75f9212319559',
        '91f8792e8e1c08e379508f254f06a306, df9420dfb0af507791c67c401ca236f6',
        '8174b1169b18edec85b8197d9e9d10b5, 88c9b8e47b0a813c2843e6d60e5eac57']
    for str in list_of_str:
        for row in uids:
            if str in row and str not in list_of_str:
                print(row)
    return df


def clean():
    df = pd.read_csv(data_file_path('raw/ipm/allegations_nopd_merged.csv'))\
        .pipe(filter_out_non_duplicates)\
        .pipe(groupby_tracking_number_and_uid)\
        .pipe(search_for_imperfect_duplicates_in_main_df)
    return df


if __name__ == '__main__':
    df = clean()
    df.to_csv(data_file_path('match/clusters_allegations_by_tracking_number_nopd_1931-2020.csv'), index=False)
