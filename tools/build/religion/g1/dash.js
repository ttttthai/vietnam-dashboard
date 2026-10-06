//import { css } from "jquery";


    $("#saveimage").click(function () {
        //html2canvas($("#contentI"), {
        //    onrendered: function (canvas) {
        //        saveAs(canvas.toDataURL(), 'dashboard_Population.png');
        //    }
        //});
        //html2canvas($("body"), { backgroundColor: null }).then(function (canvas) {

        //    document.body.appendChild(canvas);
        //});



    //    var $body1 = $('#iframe1').contents().find("body");
    //    var $body2 = $('#iframe2').contents().find("body");

    //    // clone body 1
    //    var $element = $($body1.html());

    //    // append body 2 content to body 1
    //    $body2.children().each(function () {
    //        $element.append($(this).html());
    //    });

    //    // try previwew
    //    html2canvas($element[0], {
    //        onrendered: function (canvas) {
    //            document.body.appendChild(canvas);
    //            getCanvas = canvas;
    //        }



    //    });
    //});

    })
function changeurl() {
    var iframe = parent.document.getElementById("contentI");
    var innerDoc = iframe.contentDocument || iframe.contentWindow.document;

    var currentFrame = innerDoc.location.href;



    //var new_url = document.getElementById('contentI').contentDocument.location; //$("#contentI").contents().get(0).location.href;
       // "/Your URL/" + url;
       // window.history.pushState("data", "Title", new_url);
    //document.title = url;
    //console.log(currentFrame);
}
function SetIntrotext(lang) {
    //var H = $(document).height() - 320;
    //var W = $(document).width() - 5;
    //var Wimg = $('.intro img').width();
   // var Pimg = $('.intro img').position();
   // console.log(Pimg); 

    // $('.intro_text').css('width', Wimg - 39);
    //$('.intro_text').css('margin-left', (W / 2 - Wimg/2) + 2 );
    //$('.intro_text').css('margin-top', (H / 2 - 30) + 2);
   // $('.intro').hide();
   // setTimeout(function () { $('.intro').hide(2000); }, 5000);
    $('.contin').click(function (){
        $('.intro').hide(2000);
        $('#contentI').show(1000);
        $('.thongbaochuyen').hide(1000);
    })
    var Demtime = 20;
      var refreshId = setInterval(function () {
          Demtime -= 1;
          if (lang==1) {
              $('.thongbaochuyen').html('Go to data page after ' + Demtime + 's!');
          }
          else {
              $('.thongbaochuyen').html('Đến trang dữ liệu sau ' + Demtime + ' giây!');
          }
          if (Demtime == 0) {
            $('.intro').hide(2000); 
              $('.thongbaochuyen').hide(2000); 
              $('#contentI').show(1000);
              clearInterval(refreshId);
          }
    }, 1000);

}
//$(".footer").click(function () {
//    alert('');
//    var element = $("#contentI"); // global variable
//    var getCanvas; // global variable

//    html2canvas(element, {
//        onrendered: function (canvas) {
//            $("#contentI").append(canvas);
//            getCanvas = canvas;
//        }
//    });
//});
 
   
 