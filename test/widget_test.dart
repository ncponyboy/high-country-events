import 'package:flutter_test/flutter_test.dart';
import 'package:high_country_events/main.dart';

void main() {
  testWidgets('App smoke test', (WidgetTester tester) async {
    await tester.pumpWidget(const HighCountryEventsApp());
    expect(find.byType(HighCountryEventsApp), findsOneWidget);
  });
}
